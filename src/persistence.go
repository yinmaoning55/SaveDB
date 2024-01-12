package src

import "C"
import (
	"fmt"
	"github.com/hdt3213/rdb/core"
	rdb "github.com/hdt3213/rdb/parser"
	"os"
	"sync/atomic"
)

func (server *SaveServer) loadRdbFile() error {
	rdbFile, err := os.Open(Config.RDBFilename)
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
		case rdb.ListType:
			listObj := o.(*rdb.ListObject)
			l := NewList()
			for _, v := range listObj.Values {
				l.L.Add(v)
			}
			entity = l
		case rdb.HashType:
			hashObj := o.(*rdb.HashObject)
			hash := NewHash()
			for k, v := range hashObj.Hash {
				v1 := string(v)
				hash.M[k] = &v1
			}
			entity = &hash
		case rdb.SetType:
			setObj := o.(*rdb.SetObject)
			set := NewSet()
			for _, mem := range setObj.Members {
				set.M[string(mem)] = &struct{}{}
			}
			entity = &set
		case rdb.ZSetType:
			zsetObj := o.(*rdb.ZSetObject)
			zSet := NewZSet()
			for _, e := range zsetObj.Entries {
				zSet.Z.Add(e.Member, e.Score)
			}
			entity = &zSet
		}
		if entity != nil {
			db.PutEntity(o.GetKey(), &entity)
			if o.GetExpiration() != nil {
				db.Expires[o.GetKey()] = *o.GetExpiration()
			}
			// add to aof
			//db.addAof(aof.EntityToCmd(o.GetKey(), entity).Args)
		}
		return true
	})
}

func NewPersister2(db SaveServer, filename string, load bool, fsync string) (*Persister, error) {
	return NewPersister(db, filename, load, fsync, func() SaveServer {
		return MakeAuxiliaryServer()
	})
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
