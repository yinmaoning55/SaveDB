package src

import (
	"bytes"
	"github.com/tidwall/btree"
)

var db = &saveDBTables{}

// 全局大表
type saveDBTables struct {
	Str     Str
	Hash    Hash
	Set     Set
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
	db.AllKeys = allKeys{
		btree: btree.NewBTreeG[*keyItem](func(a, b *keyItem) bool {
			return bytes.Compare(a.key, b.key) == -1
		}),
	}
}

var commandTables = []string{
	"get", "set", "delete",
	"hmset", "hdel", "hexiststofiled", "hexists", "hcard", "hgetall",
	"sadd", "smove", "shaskey", "spop", "scard", "sdiff", "sinter", "sismember", "saremembers", "smembers", "sunion",
}

func InitCommand() {
	saveCommandMap = make(map[string]saveDBCommand)
	saveCommandMap[commandTables[0]] = saveDBCommand{name: commandTables[0], saveCommandProc: Get, arity: 1}
	saveCommandMap[commandTables[1]] = saveDBCommand{name: commandTables[1], saveCommandProc: SetExc, arity: 1}
	saveCommandMap[commandTables[3]] = saveDBCommand{name: commandTables[2], saveCommandProc: Delete, arity: 1}

	saveCommandMap[commandTables[4]] = saveDBCommand{name: commandTables[3], saveCommandProc: HmSet, arity: -1}
	saveCommandMap[commandTables[5]] = saveDBCommand{name: commandTables[4], saveCommandProc: HDel, arity: -1}
	saveCommandMap[commandTables[6]] = saveDBCommand{name: commandTables[5], saveCommandProc: HExistsToFiled, arity: 2}
	saveCommandMap[commandTables[7]] = saveDBCommand{name: commandTables[6], saveCommandProc: HExists, arity: 1}
	saveCommandMap[commandTables[8]] = saveDBCommand{name: commandTables[7], saveCommandProc: HCard, arity: 1}
	saveCommandMap[commandTables[9]] = saveDBCommand{name: commandTables[8], saveCommandProc: HGetAll, arity: 1}

	saveCommandMap[commandTables[10]] = saveDBCommand{name: commandTables[9], saveCommandProc: SAdd, arity: -1}
	saveCommandMap[commandTables[11]] = saveDBCommand{name: commandTables[10], saveCommandProc: SMove, arity: -1}
	saveCommandMap[commandTables[12]] = saveDBCommand{name: commandTables[11], saveCommandProc: SHasKey, arity: 1}
	saveCommandMap[commandTables[13]] = saveDBCommand{name: commandTables[12], saveCommandProc: SPop, arity: 1}
	saveCommandMap[commandTables[14]] = saveDBCommand{name: commandTables[13], saveCommandProc: SCard, arity: 1}
	saveCommandMap[commandTables[15]] = saveDBCommand{name: commandTables[14], saveCommandProc: SDiff, arity: 2}
	saveCommandMap[commandTables[16]] = saveDBCommand{name: commandTables[15], saveCommandProc: SInter, arity: 2}
	saveCommandMap[commandTables[17]] = saveDBCommand{name: commandTables[16], saveCommandProc: SIsMember, arity: 2}
	saveCommandMap[commandTables[18]] = saveDBCommand{name: commandTables[17], saveCommandProc: SAreMembers, arity: -1}
	saveCommandMap[commandTables[19]] = saveDBCommand{name: commandTables[18], saveCommandProc: SMembers, arity: 1}
	saveCommandMap[commandTables[19]] = saveDBCommand{name: commandTables[19], saveCommandProc: SUnion, arity: 2}
}
