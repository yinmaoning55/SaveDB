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
	//暂停开始/结束 重写进程
	pausingAof sync.Mutex
	currentDB  int
	listeners  map[Listener]struct{}
	// reuse cmdLine buffer
	buffer         []CmdLine
	loading        *atomic.Bool
	usedMemorySize uint64
}

func (server *SaveServer) loadRdbFile() error {
	server.persister.loading.Store(true)
	rdbFile, err := os.Open(GetRDBFilePath())
	if err != nil {
		return fmt.Errorf("open rdb file failed " + err.Error())
	}
	defer func() {
		_ = rdbFile.Close()
		server.persister.loading.Store(false)
	}()
	decoder := rdb.NewDecoder(rdbFile)
	err = server.LoadRDB(decoder)
	if err != nil {
		return fmt.Errorf("load rdb file failed " + err.Error())
	}

	return nil
}

func (server *SaveServer) LoadRDB(dec *core.Decoder) error {
	server.persister.loading.Store(true)
	defer server.persister.loading.Store(false)
	f := dec.Parse(func(o rdb.RedisObject) bool {
		db := server.FindDB(o.GetDBIndex())
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
			entity = hash
		case rdb.SetType:
			setObj := o.(*rdb.SetObject)
			set := NewSet()
			for _, mem := range setObj.Members {
				set.M[string(mem)] = &struct{}{}
			}
			db.PutKey(o.GetKey(), TypeSet)
			entity = set
		case rdb.ZSetType:
			zsetObj := o.(*rdb.ZSetObject)
			zSet := NewZSet()
			for _, e := range zsetObj.Entries {
				zSet.Z.Add(e.Member, e.Score)
			}
			db.PutKey(o.GetKey(), TypeZSet)
			entity = zSet
		}
		if entity != nil {
			db.PutEntity(o.GetKey(), entity)
			if o.GetExpiration() != nil {
				PutExpire(db, o.GetKey(), *o.GetExpiration())
			}
			// add to aof
			args := EntityToCmd(o.GetKey(), entity).Args
			//启动时这里的addAof方法是个空方法
			db.addAof(args)
		}
		return true
	})

	return f
}

func NewPersister2(db SaveServer, fsync string) (*Persister, error) {
	return NewPersister(db, fsync, func() SaveServer {
		return MakeTempServer()
	})
}
func NewPersister(db SaveServer, fsync string, tmpDBMaker func() SaveServer) (*Persister, error) {
	persister := &Persister{}
	persister.aofFsync = strings.ToLower(fsync)
	persister.db = db
	persister.tmpDBMaker = tmpDBMaker
	persister.currentDB = 0
	holder := &atomic.Bool{}
	holder.Store(false)
	persister.loading = holder
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

func MakeTempServer() SaveServer {
	mdb := SaveServer{}
	mdb.Dbs = make([]*atomic.Value, dbsSize)
	for i := range mdb.Dbs {
		holder := &atomic.Value{}
		holder.Store(makeDB(i))
		mdb.Dbs[i] = holder
	}
	return mdb
}
