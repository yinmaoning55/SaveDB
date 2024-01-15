package src

import "C"
import (
	"context"
	"fmt"
	"github.com/hdt3213/rdb/core"
	rdb "github.com/hdt3213/rdb/parser"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

type Persister struct {
	ctx        context.Context
	cancel     context.CancelFunc
	db         SaveServer
	tmpDBMaker func() SaveServer
	// aofChan is the channel to receive aof payload(listenCmd will send payload to this channel)
	aofChan chan *payload
	// aofFile is the file handler of aof file
	aofFile *os.File
	// aofFsync is the strategy of fsync
	aofFsync string
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shut down
	aofFinished chan struct{}
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.Mutex
	currentDB  int
	listeners  map[Listener]struct{}
	// reuse cmdLine buffer
	buffer []CmdLine
}

func (server *SaveServer) loadRdbFile() error {
	rdbFile, err := os.Open(GetRDBFilePath())
	if err != nil {
		return fmt.Errorf("open rdb file failed " + err.Error())
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	decoder := rdb.NewDecoder(rdbFile)
	err = server.LoadRDB(decoder)
	if err != nil {
		return fmt.Errorf("load rdb file failed " + err.Error())
	}
	return nil
}

func (server *SaveServer) LoadRDB(dec *core.Decoder) error {
	return dec.Parse(func(o rdb.RedisObject) bool {
		db := FindDB(o.GetDBIndex())
		var entity any
		switch o.GetType() {
		case rdb.StringType:
			str := o.(*rdb.StringObject)
			entity = str.Value
			db.PutKey(o.GetKey(), TypeStr)
		case rdb.ListType:
			listObj := o.(*rdb.ListObject)
			l := NewList()
			for _, v := range listObj.Values {
				l.L.Add(v)
			}
			db.PutKey(o.GetKey(), TypeList)
			entity = l
		case rdb.HashType:
			hashObj := o.(*rdb.HashObject)
			hash := NewHash()
			for k, v := range hashObj.Hash {
				v1 := string(v)
				hash.M[k] = &v1
			}
			db.PutKey(o.GetKey(), TypeHash)
			entity = &hash
		case rdb.SetType:
			setObj := o.(*rdb.SetObject)
			set := NewSet()
			for _, mem := range setObj.Members {
				set.M[string(mem)] = &struct{}{}
			}
			db.PutKey(o.GetKey(), TypeSet)
			entity = &set
		case rdb.ZSetType:
			zsetObj := o.(*rdb.ZSetObject)
			zSet := NewZSet()
			for _, e := range zsetObj.Entries {
				zSet.Z.Add(e.Member, e.Score)
			}
			db.PutKey(o.GetKey(), TypeZSet)
			entity = &zSet
		}
		if entity != nil {
			db.PutEntity(o.GetKey(), &entity)
			if o.GetExpiration() != nil {
				db.Expires[o.GetKey()] = *o.GetExpiration()
			}
			// add to aof
			db.addAof(EntityToCmd(o.GetKey(), entity).Args)
		}
		return true
	})
}

func NewPersister2(db SaveServer, load bool, fsync string) (*Persister, error) {
	return NewPersister(db, load, fsync, func() SaveServer {
		return MakeAuxiliaryServer()
	})
}
func NewPersister(db SaveServer, load bool, fsync string, tmpDBMaker func() SaveServer) (*Persister, error) {
	persister := &Persister{}
	persister.aofFsync = strings.ToLower(fsync)
	persister.db = db
	persister.tmpDBMaker = tmpDBMaker
	persister.currentDB = 0
	// load aof file if needed
	if load {
		persister.LoadAof(0)
	}
	//打开文件时的标志位，使用位掩码
	//os.O_APPEND: 将文件指针设置为文件末尾，在文件中追加数据。
	//os.O_CREATE: 如果文件不存在，则创建文件。
	//os.O_RDWR: 以读写方式打开文件。
	if Config.AppendOnly {
		aofFile, err := os.OpenFile(GetAofFilePath(), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		persister.aofFile = aofFile
		persister.aofChan = make(chan *payload, aofQueueSize)
		persister.aofFinished = make(chan struct{})
	}
	persister.listeners = make(map[Listener]struct{})
	// start aof goroutine to write aof file in background and fsync periodically if needed (see fsyncEverySecond)
	go func() {
		persister.listenCmd()
	}()
	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel
	// fsync every second if needed
	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	return persister, nil
}
func (server *SaveServer) AddAof(dbIndex int, cmdLine CmdLine) {
	if server.persister != nil {
		server.persister.SaveCmdLine(dbIndex, cmdLine)
	}
}

func (server *SaveServer) bindPersister(aofHandler *Persister) {
	server.persister = aofHandler
	// bind SaveCmdLine
	for _, db := range server.Dbs {
		singleDB := db.Load().(*SaveDBTables)
		singleDB.addAof = func(line CmdLine) {
			if Config.AppendOnly { // config may be changed during runtime
				server.persister.SaveCmdLine(singleDB.index, line)
			}
		}
	}
}

func MakeAuxiliaryServer() SaveServer {
	mdb := SaveServer{}
	mdb.Dbs = make([]*atomic.Value, dbsSize)
	for i := range mdb.Dbs {
		holder := &atomic.Value{}
		holder.Store(makeDB(i))
		mdb.Dbs[i] = holder
	}
	return mdb
}
