package src

import (
	"savedb/src/data"
	"time"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
)

var saveCommandMap map[string]saveDBCommand

// 所有的命令 基本上和redis一样
type saveDBCommand struct {
	name            string                                       //参数名字
	saveCommandProc func(db *saveDBTables, args []string) Result //执行的函数
	arity           int                                          //参数个数
	funcKeys        KeysLockFunc                                 //获取命令中所有用于加锁的key
}

type KeysLockFunc func(args []string) ([]string, []string)

// 全局大表
type saveDBTables struct {
	Data    *data.ConcurrentDict
	Expires map[string]time.Time //带有过期的key统一管理
	AllKeys                      //缓存淘汰
}
type SaveObject struct {
	dataType byte    //key的数据类型
	lru      int64   //redisObject的LRU时间， 毫秒
	refCount int16   //redisObject的引用计数
	prt      *string //指向值的指针，8个字节

}

func init() {
	Server.Db = &saveDBTables{}
	Server.CreateSaveDB()
}

func (s *saveServer) CreateSaveDB() {
	s.Db.Data = data.MakeConcurrent(dataDictSize)
	s.Db.Expires = make(map[string]time.Time)
	s.Db.AllKeys = NewLKeys()
}
func NewSaveObject(key *string, keyType byte) *SaveObject {
	o := &SaveObject{
		dataType: keyType,
		lru:      time.Now().Unix(),
		prt:      key,
	}
	return o
}

//func BGSaveRDB(db *saveDBTables, args [][]byte) Result {
//	if db.persister == nil {
//		return protocol.MakeErrReply("please enable aof before using save")
//	}
//	go func() {
//		defer func() {
//			if err := recover(); err != nil {
//				logger.Error(err)
//			}
//		}()
//		rdbFilename := config.Properties.RDBFilename
//		if rdbFilename == "" {
//			rdbFilename = "dump.rdb"
//		}
//		err := db.persister.GenerateRDB(rdbFilename)
//		if err != nil {
//			logger.Error(err)
//		}
//	}()
//	return protocol.MakeStatusReply("Background saving started")
//}

func (db *saveDBTables) Locks(readKeys, writeKeys []string) {
	if readKeys == nil && writeKeys == nil {
		return
	}
	db.Data.RWLocks(writeKeys, readKeys)
}

func (db *saveDBTables) UnLocks(readKeys, writeKeys []string) {
	if readKeys == nil && writeKeys == nil {
		return
	}
	db.Data.RWUnLocks(writeKeys, readKeys)
}

func InitCommand() {
	saveCommandMap = make(map[string]saveDBCommand)
	saveCommandMap["get"] = saveDBCommand{name: "get", saveCommandProc: Get, arity: 1, funcKeys: readFirstKey}
	saveCommandMap["set"] = saveDBCommand{name: "set", saveCommandProc: SetExc, arity: 2, funcKeys: writeFirstKey}
	saveCommandMap["delete"] = saveDBCommand{name: "del", saveCommandProc: Del, arity: -1, funcKeys: writeAllKeys}

	saveCommandMap["keys"] = saveDBCommand{name: "keys", saveCommandProc: Keys, arity: 1}
	saveCommandMap["exists"] = saveDBCommand{name: "exists", saveCommandProc: Exists, arity: 1, funcKeys: readFirstKey}

	saveCommandMap["hmset"] = saveDBCommand{name: "hmset", saveCommandProc: HmSet, arity: -1, funcKeys: writeFirstKey}
	saveCommandMap["hget"] = saveDBCommand{name: "hget", saveCommandProc: HMGet, arity: 2, funcKeys: writeFirstKey}
	saveCommandMap["hdel"] = saveDBCommand{name: "hdel", saveCommandProc: HDel, arity: -1, funcKeys: writeFirstKey}
	saveCommandMap["hexists"] = saveDBCommand{name: "hexists", saveCommandProc: HExists, arity: 2, funcKeys: readAllKeys}
	saveCommandMap["hcard"] = saveDBCommand{name: "hcard", saveCommandProc: HCard, arity: 1, funcKeys: readFirstKey}
	saveCommandMap["hgetall"] = saveDBCommand{name: "hgetall", saveCommandProc: HGetAll, arity: 1, funcKeys: readFirstKey}

	saveCommandMap["sadd"] = saveDBCommand{name: "sadd", saveCommandProc: SAdd, arity: -1, funcKeys: writeFirstKey}
	saveCommandMap["smove"] = saveDBCommand{name: "smove", saveCommandProc: SMove, arity: -1, funcKeys: writeFirstKey}
	saveCommandMap["shaskey"] = saveDBCommand{name: "shaskey", saveCommandProc: SHasKey, arity: 1, funcKeys: writeFirstKey}
	saveCommandMap["spop"] = saveDBCommand{name: "spop", saveCommandProc: SPop, arity: 1, funcKeys: writeFirstKey}
	saveCommandMap["scard"] = saveDBCommand{name: "scard", saveCommandProc: SCard, arity: 1, funcKeys: readFirstKey}
	saveCommandMap["sdiff"] = saveDBCommand{name: "sdiff", saveCommandProc: SDiff, arity: 2, funcKeys: readAllKeys}
	saveCommandMap["sinter"] = saveDBCommand{name: "sinter", saveCommandProc: SInter, arity: 2, funcKeys: writeAllKeys}
	saveCommandMap["sismember"] = saveDBCommand{name: "sismember", saveCommandProc: SIsMember, arity: 2, funcKeys: readFirstKey}
	saveCommandMap["smembers"] = saveDBCommand{name: "smembers", saveCommandProc: SMembers, arity: 1, funcKeys: readFirstKey}
	saveCommandMap["sunion"] = saveDBCommand{name: "sunion", saveCommandProc: SUnion, arity: 2, funcKeys: readAllKeys}

	saveCommandMap["llen"] = saveDBCommand{name: "llen", saveCommandProc: LLen, arity: 1, funcKeys: readFirstKey}
	saveCommandMap["lpop"] = saveDBCommand{name: "lpop", saveCommandProc: LPop, arity: 1, funcKeys: writeFirstKey}
	saveCommandMap["lpush"] = saveDBCommand{name: "lpush", saveCommandProc: LPush, arity: 2, funcKeys: writeFirstKey}
	saveCommandMap["lpushx"] = saveDBCommand{name: "lpushx", saveCommandProc: LPushX, arity: 2, funcKeys: writeFirstKey}
	saveCommandMap["lrange"] = saveDBCommand{name: "lrange", saveCommandProc: LRange, arity: 3, funcKeys: readFirstKey}
	saveCommandMap["lrem"] = saveDBCommand{name: "lrem", saveCommandProc: LRem, arity: 3, funcKeys: writeFirstKey}
	saveCommandMap["lset"] = saveDBCommand{name: "lset", saveCommandProc: LSet, arity: 3, funcKeys: writeFirstKey}
	saveCommandMap["rpop"] = saveDBCommand{name: "rpop", saveCommandProc: RPop, arity: 1, funcKeys: writeFirstKey}
	saveCommandMap["rpoplpush"] = saveDBCommand{name: "rpoplpush", saveCommandProc: RPopLPush, arity: 2, funcKeys: writeAllKeys}
	saveCommandMap["rpush"] = saveDBCommand{name: "rpush", saveCommandProc: RPush, arity: -1, funcKeys: writeFirstKey}
	saveCommandMap["rpushx"] = saveDBCommand{name: "rpushx", saveCommandProc: RPushX, arity: -1, funcKeys: writeFirstKey}
	saveCommandMap["ltrim"] = saveDBCommand{name: "ltrim", saveCommandProc: LTrim, arity: 3, funcKeys: writeFirstKey}
	saveCommandMap["linsert"] = saveDBCommand{name: "linsert", saveCommandProc: LInsert, arity: 4, funcKeys: writeFirstKey}

	saveCommandMap["zadd"] = saveDBCommand{name: "zadd", saveCommandProc: ZAdd, arity: -1, funcKeys: writeFirstKey}
	saveCommandMap["zscore"] = saveDBCommand{name: "zscore", saveCommandProc: ZScore, arity: 2, funcKeys: readFirstKey}
	saveCommandMap["zrank"] = saveDBCommand{name: "zrank", saveCommandProc: ZRank, arity: 2, funcKeys: readFirstKey}
	saveCommandMap["zrevrank"] = saveDBCommand{name: "zrevrank", saveCommandProc: ZRevRank, arity: 2, funcKeys: readFirstKey}
	saveCommandMap["zcard"] = saveDBCommand{name: "zcard", saveCommandProc: ZCard, arity: 1, funcKeys: readFirstKey}
	saveCommandMap["zrange"] = saveDBCommand{name: "zrange", saveCommandProc: ZRange, arity: -1, funcKeys: readFirstKey}
	saveCommandMap["zrevrange"] = saveDBCommand{name: "zrevrange", saveCommandProc: ZRevRange, arity: -1, funcKeys: readFirstKey}
	saveCommandMap["zcount"] = saveDBCommand{name: "zcount", saveCommandProc: ZCount, arity: 3, funcKeys: readFirstKey}
	saveCommandMap["zrangebyscore"] = saveDBCommand{name: "zrangebyscore", saveCommandProc: ZRangeByScore, arity: -1, funcKeys: readFirstKey}
	saveCommandMap["zrevrangebyscore"] = saveDBCommand{name: "ZRevRangeByScore", saveCommandProc: ZRevRangeByScore, arity: -1, funcKeys: readFirstKey}
	saveCommandMap["zremrangebyscore"] = saveDBCommand{name: "zremrangebyscore", saveCommandProc: ZRemRangeByScore, arity: -1, funcKeys: readFirstKey}
	saveCommandMap["zpopMin"] = saveDBCommand{name: "zpopMin", saveCommandProc: ZPopMin, arity: 2, funcKeys: writeFirstKey}
	saveCommandMap["zrem"] = saveDBCommand{name: "zrem", saveCommandProc: ZRem, arity: -1, funcKeys: writeFirstKey}
	saveCommandMap["zincrby"] = saveDBCommand{name: "zincrby", saveCommandProc: ZIncrBy, arity: 3, funcKeys: writeFirstKey}
	saveCommandMap["zlexcount"] = saveDBCommand{name: "zlexcount", saveCommandProc: ZLexCount, arity: 3, funcKeys: readFirstKey}
	saveCommandMap["zrangebylex"] = saveDBCommand{name: "zrangebylex", saveCommandProc: ZRangeByLex, arity: -1, funcKeys: readFirstKey}
	saveCommandMap["zremrangebylex"] = saveDBCommand{name: "zremrangebylex", saveCommandProc: ZRemRangeByLex, arity: 3, funcKeys: readFirstKey}

	saveCommandMap["expire"] = saveDBCommand{name: "expire", saveCommandProc: Expire, arity: 2, funcKeys: writeFirstKey}
	saveCommandMap["ttl"] = saveDBCommand{name: "ttl", saveCommandProc: TTL, arity: 1, funcKeys: readFirstKey}

}
