package src

import (
	"bytes"
	"github.com/tidwall/btree"
	"strconv"
	"time"
)

type AllKeys struct {
	keys *btree.BTreeG[*keyItem]
}
type keyItem struct {
	key     []byte
	saveObj *SaveObject
}

func NewLKeys() AllKeys {
	keys := AllKeys{
		keys: btree.NewBTreeG[*keyItem](func(a, b *keyItem) bool {
			return bytes.Compare(a.key, b.key) == -1
		}),
	}
	return keys
}
func Expire(db *saveDBTables, args []string) Result {
	key := args[0]
	expire, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return CreateStrResult(C_ERR, "args error, cant transfer int")
	}
	if !db.AllKeys.Exist(key) {
		return CreateStrResult(C_ERR, "key not exist")
	}
	ttl := time.Duration(expire*1000) * time.Millisecond
	expireAt := time.Now().Add(ttl)
	db.Expires[key] = expireAt
	AddTimer(expireAt, key, func() {
		DelExpireKey(args)
	})
	return CreateStrResult(C_OK, OK_STR)
}

// 发送到主协程处理
func DelExpireKey(args []string) {
	command := "delete"
	msg := &Message{Command: &command, Args: args}
	server.Read <- msg
}
func TTL(db *saveDBTables, args []string) Result {
	key := args[0]
	value, ok := db.Expires[key]
	if !ok {
		return CreateStrResult(C_OK, "-2")
	}
	nowTime := time.Now().Unix()
	ttl := value.Unix() - nowTime
	return CreateStrResult(C_OK, strconv.Itoa(int(ttl)))
}

func Delete(db *saveDBTables, args []string) Result {
	key := args[0]
	if !db.AllKeys.Exist(key) {
		return CreateStrResult(C_ERR, "key is exist")
	}
	keyType := db.AllKeys.GetKey(key).dataType
	switch keyType {
	case TypeStr:
		DelStr(db, args)
		break
	case TypeHash:
		DelHash(db, key)
		break
	case TypeSet:
		DelSet(db, key)
		break
	case TypeZSet:
		DelZSet(db, key)
		break
	case TypeList:
		DelList(db, key)
		break
	}
	return CreateStrResult(C_OK, OK_STR)
}

func (a *AllKeys) PutKey(key string, keyType byte) {
	ki := &keyItem{
		key:     StringToBytes(key),
		saveObj: NewSaveObject(&key, keyType),
	}
	a.keys.Set(ki)
}

func (a *AllKeys) RemoveKey(db *saveDBTables, key string) {
	ki := &keyItem{
		key: StringToBytes(key),
	}
	a.keys.Delete(ki)
	delete(db.Expires, key)
}

func (a *AllKeys) ActivateKey(key string) {
	ki := &keyItem{
		key: StringToBytes(key),
	}
	value, ok := a.keys.Get(ki)
	if !ok {
		return
	}
	value.saveObj.lru = time.Now().Unix()
	//多线程需要保证线程安全
	value.saveObj.refCount += 1
}

func (a *AllKeys) Exist(key string) bool {
	ki := &keyItem{
		key: StringToBytes(key),
	}
	_, ok := a.keys.Get(ki)
	return ok
}
func (a *AllKeys) GetKey(key string) *SaveObject {
	ki := &keyItem{
		key: StringToBytes(key),
	}
	value, ok := a.keys.Get(ki)
	if !ok {
		return nil
	}
	return value.saveObj
}
