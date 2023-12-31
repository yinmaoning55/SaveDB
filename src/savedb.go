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
	allKeys allKeys
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
	db.allKeys = allKeys{
		btree: btree.NewBTreeG[*keyItem](func(a, b *keyItem) bool {
			return bytes.Compare(a.key, b.key) == -1
		}),
	}
}
