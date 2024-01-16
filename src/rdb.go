package src

import (
	rdb "github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
	"os"
	"savedb/src/data"
	"savedb/src/log"
	"strconv"
	"time"
)

func (persister *Persister) GenerateRDB(rdbFilename string) error {
	ctx, err := persister.startGenerateRDB(nil, nil)
	if err != nil {
		return err
	}
	err = persister.generateRDB(ctx)
	if err != nil {
		return err
	}
	err = ctx.tmpFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(ctx.tmpFile.Name(), GetRDBFilePath())
	if err != nil {
		return err
	}
	log.SaveDBLogger.Infof("rdb file create successful.")
	return nil
}

func (persister *Persister) GenerateRDBForReplication(rdbFilename string, listener Listener, hook func()) error {
	ctx, err := persister.startGenerateRDB(listener, hook)
	if err != nil {
		return err
	}

	err = persister.generateRDB(ctx)
	if err != nil {
		return err
	}
	err = ctx.tmpFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(ctx.tmpFile.Name(), rdbFilename)
	if err != nil {
		return err
	}
	return nil
}

func (persister *Persister) startGenerateRDB(newListener Listener, hook func()) (*RewriteCtx, error) {
	persister.pausingAof.Lock() // pausing aof
	defer persister.pausingAof.Unlock()
	//将aof文件的内容同步落盘
	err := persister.aofFile.Sync()
	if err != nil {
		log.SaveDBLogger.Warn("fsync failed")
		return nil, err
	}

	//获取文件大小
	fileInfo, _ := os.Stat(GetAofFilePath())
	filesize := fileInfo.Size()
	// create tmp file
	file, err := os.CreateTemp(Config.Dir, "*.rdb")
	if err != nil {
		log.SaveDBLogger.Warn("tmp file create failed")
		return nil, err
	}
	if newListener != nil {
		persister.listeners[newListener] = struct{}{}
	}
	if hook != nil {
		hook()
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
	}, nil
}

func (persister *Persister) generateRDB(ctx *RewriteCtx) error {
	//使用aof重新生成临时的dbs 然后遍历备份
	tmpPersister := persister.newRewriteHandler()
	//todo 暂时只重放aof文件
	tmpPersister.LoadAof(0)

	encoder := rdb.NewEncoder(ctx.tmpFile).EnableCompress()
	err := encoder.WriteHeader()
	if err != nil {
		return err
	}
	auxMap := map[string]string{
		"redis-ver":    "6.0.0",
		"redis-bits":   "64",
		"aof-preamble": "0",
		"ctime":        strconv.FormatInt(time.Now().Unix(), 10),
	}

	// change aof preamble
	if Config.AofUseRdbPreamble {
		auxMap["aof-preamble"] = "1"
	}

	for k, v := range auxMap {
		err := encoder.WriteAux(k, v)
		if err != nil {
			return err
		}
	}

	for i := 0; i < dbsSize; i++ {
		db := tmpPersister.db.Dbs[i].Load().(*SaveDBTables)
		keyCount := db.keys.Len()
		ttlCount := len(db.Expires)
		if keyCount == 0 {
			continue
		}
		err = encoder.WriteDBHeader(uint(i), uint64(keyCount), uint64(ttlCount))
		if err != nil {
			return err
		}
		// dump db
		var err2 error
		db.ForEach(i, func(key string, entity any, expiration *time.Time) bool {
			var opts []interface{}
			if expiration != nil {
				opts = append(opts, rdb.WithTTL(uint64(expiration.UnixNano()/1e6)))
			}
			switch obj := entity.(type) {
			case []byte:
				err = encoder.WriteStringObject(key, obj, opts...)
			case *List:
				vals := make([][]byte, 0, obj.L.Len())
				obj.L.ForEach(func(i int, v interface{}) bool {
					bytes, _ := v.([]byte)
					vals = append(vals, bytes)
					return true
				})
				err = encoder.WriteListObject(key, vals, opts...)
			case *Set:
				vals := make([][]byte, 0, len(obj.M))
				for key, _ := range obj.M {
					vals = append(vals, []byte(key))
				}
				err = encoder.WriteSetObject(key, vals, opts...)
			case *Hash:
				hash := make(map[string][]byte)
				for key, val := range obj.M {
					bytes := []byte(*val)
					hash[key] = bytes
				}
				err = encoder.WriteHashMapObject(key, hash, opts...)
			case *ZSet:
				var entries []*model.ZSetEntry
				obj.Z.ForEachByRank(int64(0), obj.Z.Len(), true, func(element *data.Element) bool {
					entries = append(entries, &model.ZSetEntry{
						Member: element.Member,
						Score:  element.Score,
					})
					return true
				})
				err = encoder.WriteZSetObject(key, entries, opts...)
			}
			if err != nil {
				err2 = err
				return false
			}
			return true
		})
		if err2 != nil {
			return err2
		}
	}
	err = encoder.WriteEnd()
	if err != nil {
		return err
	}
	return nil
}
