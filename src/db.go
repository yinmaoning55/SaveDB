package src

import (
	"savedb/src/data"
	"time"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
)

var db = &saveDBTables{}

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

func CreateSaveDB() {
	db.Data = data.MakeConcurrent(dataDictSize)
	db.Expires = make(map[string]time.Time)
	db.AllKeys = NewLKeys()
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

func InitCommand() {
	saveCommandMap = make(map[string]saveDBCommand)
	saveCommandMap["get"] = saveDBCommand{name: "get", saveCommandProc: Get, arity: 1}
	saveCommandMap["set"] = saveDBCommand{name: "set", saveCommandProc: SetExc, arity: 2}
	saveCommandMap["delete"] = saveDBCommand{name: "delete", saveCommandProc: Delete, arity: 1}

	saveCommandMap["keys"] = saveDBCommand{name: "keys", saveCommandProc: Keys, arity: 1}

	saveCommandMap["hmset"] = saveDBCommand{name: "hmset", saveCommandProc: HmSet, arity: -1}
	saveCommandMap["hget"] = saveDBCommand{name: "hget", saveCommandProc: HMGet, arity: 2}
	saveCommandMap["hdel"] = saveDBCommand{name: "hdel", saveCommandProc: HDel, arity: -1}
	saveCommandMap["hexiststofiled"] = saveDBCommand{name: "hexiststofiled", saveCommandProc: HExistsToFiled, arity: 2}
	saveCommandMap["hexists"] = saveDBCommand{name: "hexists", saveCommandProc: HExists, arity: 1}
	saveCommandMap["hcard"] = saveDBCommand{name: "hcard", saveCommandProc: HCard, arity: 1}
	saveCommandMap["hgetall"] = saveDBCommand{name: "hgetall", saveCommandProc: HGetAll, arity: 1}

	saveCommandMap["sadd"] = saveDBCommand{name: "sadd", saveCommandProc: SAdd, arity: -1}
	saveCommandMap["smove"] = saveDBCommand{name: "smove", saveCommandProc: SMove, arity: -1}
	saveCommandMap["shaskey"] = saveDBCommand{name: "shaskey", saveCommandProc: SHasKey, arity: 1}
	saveCommandMap["spop"] = saveDBCommand{name: "spop", saveCommandProc: SPop, arity: 1}
	saveCommandMap["scard"] = saveDBCommand{name: "scard", saveCommandProc: SCard, arity: 1}
	saveCommandMap["sdiff"] = saveDBCommand{name: "sdiff", saveCommandProc: SDiff, arity: 2}
	saveCommandMap["sinter"] = saveDBCommand{name: "sinter", saveCommandProc: SInter, arity: 2}
	saveCommandMap["sismember"] = saveDBCommand{name: "sismember", saveCommandProc: SIsMember, arity: 2}
	saveCommandMap["saremembers"] = saveDBCommand{name: "saremembers", saveCommandProc: SAreMembers, arity: -1}
	saveCommandMap["smembers"] = saveDBCommand{name: "smembers", saveCommandProc: SMembers, arity: 1}
	saveCommandMap["sunion"] = saveDBCommand{name: "sunion", saveCommandProc: SUnion, arity: 2}

	saveCommandMap["llen"] = saveDBCommand{name: "llen", saveCommandProc: LLen, arity: 1}
	saveCommandMap["lpop"] = saveDBCommand{name: "lpop", saveCommandProc: LPop, arity: 1}
	saveCommandMap["lpush"] = saveDBCommand{name: "lpush", saveCommandProc: LPush, arity: 2}
	saveCommandMap["lpushx"] = saveDBCommand{name: "lpushx", saveCommandProc: LPushX, arity: 2}
	saveCommandMap["lrange"] = saveDBCommand{name: "lrange", saveCommandProc: LRange, arity: 3}
	saveCommandMap["lrem"] = saveDBCommand{name: "lrem", saveCommandProc: LRem, arity: 3}
	saveCommandMap["lset"] = saveDBCommand{name: "lset", saveCommandProc: LSet, arity: 3}
	saveCommandMap["rpop"] = saveDBCommand{name: "rpop", saveCommandProc: RPop, arity: 1}
	saveCommandMap["rpoplpush"] = saveDBCommand{name: "rpoplpush", saveCommandProc: RPopLPush, arity: 2}
	saveCommandMap["rpush"] = saveDBCommand{name: "rpush", saveCommandProc: RPush, arity: -1}
	saveCommandMap["rpushx"] = saveDBCommand{name: "rpushx", saveCommandProc: RPushX, arity: -1}
	saveCommandMap["ltrim"] = saveDBCommand{name: "ltrim", saveCommandProc: LTrim, arity: 3}
	saveCommandMap["linsert"] = saveDBCommand{name: "linsert", saveCommandProc: LInsert, arity: 4}

	saveCommandMap["zadd"] = saveDBCommand{name: "zadd", saveCommandProc: ZAdd, arity: -1}
	saveCommandMap["zscore"] = saveDBCommand{name: "zscore", saveCommandProc: ZScore, arity: 2}
	saveCommandMap["zrank"] = saveDBCommand{name: "zrank", saveCommandProc: ZRank, arity: 2}
	saveCommandMap["zrevrank"] = saveDBCommand{name: "zrevrank", saveCommandProc: ZRevRank, arity: 2}
	saveCommandMap["zcard"] = saveDBCommand{name: "zcard", saveCommandProc: ZCard, arity: 1}
	saveCommandMap["zrange"] = saveDBCommand{name: "zrange", saveCommandProc: ZRange, arity: -1}
	saveCommandMap["zrevrange"] = saveDBCommand{name: "zrevrange", saveCommandProc: ZRevRange, arity: -1}
	saveCommandMap["zcount"] = saveDBCommand{name: "zcount", saveCommandProc: ZCount, arity: 3}
	saveCommandMap["zrangebyscore"] = saveDBCommand{name: "zrangebyscore", saveCommandProc: ZRangeByScore, arity: -1}
	saveCommandMap["zrevrangebyscore"] = saveDBCommand{name: "ZRevRangeByScore", saveCommandProc: ZRevRangeByScore, arity: -1}
	saveCommandMap["zremrangebyscore"] = saveDBCommand{name: "zremrangebyscore", saveCommandProc: ZRemRangeByScore, arity: -1}
	saveCommandMap["zpopMin"] = saveDBCommand{name: "zpopMin", saveCommandProc: ZPopMin, arity: 2}
	saveCommandMap["zrem"] = saveDBCommand{name: "zrem", saveCommandProc: ZRem, arity: -1}
	saveCommandMap["zincrby"] = saveDBCommand{name: "zincrby", saveCommandProc: ZIncrBy, arity: 3}
	saveCommandMap["zlexcount"] = saveDBCommand{name: "zlexcount", saveCommandProc: ZLexCount, arity: 3}
	saveCommandMap["zrangebylex"] = saveDBCommand{name: "zrangebylex", saveCommandProc: ZRangeByLex, arity: -1}
	saveCommandMap["zremrangebylex"] = saveDBCommand{name: "zremrangebylex", saveCommandProc: ZRemRangeByLex, arity: 3}
	saveCommandMap["zrevrangebylex"] = saveDBCommand{name: "zrevrangebylex", saveCommandProc: ZRevRangeByLex, arity: -1}

	saveCommandMap["expire"] = saveDBCommand{name: "expire", saveCommandProc: Expire, arity: 2}
	saveCommandMap["ttl"] = saveDBCommand{name: "ttl", saveCommandProc: TTL, arity: 1}

}
