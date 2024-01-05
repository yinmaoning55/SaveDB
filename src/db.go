package src

import (
	"bytes"
	"github.com/tidwall/btree"
)

var db = &saveDBTables{}

// 全局大表
type saveDBTables struct {
	Str
	Hash
	Set
	List
	Expires map[string]uint64 //带有过期的key统一管理
	AllKeys allKeys           //缓存淘汰
}
type saveObject struct {
	dataType byte    //key的数据类型
	lru      uint32  //redisObject的LRU时间，LRU_BITS为24个bits
	refCount int     //redisObject的引用计数，4个字节
	prt      *string //指向值的指针，8个字节

}
type allKeys struct {
	btree *btree.BTreeG[*keyItem]
}
type keyItem struct {
	key     []byte
	saveObj *saveObject
}

func CreateSaveDB() {
	db.Str = *NewString()
	db.Expires = make(map[string]uint64)
	db.Set = *NewSet()
	db.Hash = *NewHash()
	db.List = *NewList()
	db.AllKeys = allKeys{
		btree: btree.NewBTreeG[*keyItem](func(a, b *keyItem) bool {
			return bytes.Compare(a.key, b.key) == -1
		}),
	}
}

func InitCommand() {
	saveCommandMap = make(map[string]saveDBCommand)
	saveCommandMap["get"] = saveDBCommand{name: "get", saveCommandProc: Get, arity: 1}
	saveCommandMap["set"] = saveDBCommand{name: "set", saveCommandProc: SetExc, arity: 2}
	saveCommandMap["delete"] = saveDBCommand{name: "delete", saveCommandProc: Delete, arity: 1}

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
}
