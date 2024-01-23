package src

import (
	"bytes"
	"github.com/tidwall/btree"
	"regexp"
	"savedb/src/timewheel"
	"strconv"
	"strings"
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
func Expire(db *SaveDBTables, args []string) Result {
	key := args[0]
	expire, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return CreateStrResult(CErr, "args error, cant transfer int")
	}
	if !db.AllKeys.Exist(key) {
		return CreateStrResult(CErr, "key not exist")
	}
	nowTime := time.Now().UnixMilli()
	if nowTime > expire {
		return Result{}
	}
	ttl := time.Unix(expire/1000, 0)
	PutExpire(db, key, ttl)
	db.addAof(MakeExpireCmd(key, ttl).Args)
	return CreateStrResult(COk, OkStr)
}
func PutExpire(db *SaveDBTables, key string, time time.Time) {
	db.Expires[key] = time
	args := make([]string, 1)
	args = append(args, key)
	timewheel.AddTimer(time, key, func() {
		keys := make([]string, 0)
		keys = append(keys, key)
		db.Locks(nil, keys)
		defer func() {
			db.UnLocks(nil, keys)
		}()
		Del(db, args)
	})
}
func TTL(db *SaveDBTables, args []string) Result {
	key := args[0]
	value, ok := db.Expires[key]
	if !ok {
		return CreateStrResult(COk, "-2")
	}
	nowTime := time.Now().Unix()
	ttl := value.Unix() - nowTime
	return CreateStrResult(COk, strconv.Itoa(int(ttl)))
}

func Del(db *SaveDBTables, args []string) Result {
	var deleted int
	for _, k := range args {
		if !db.AllKeys.Exist(k) {
			continue
		}
		db.Data.RemoveWithLock(k)
		db.AllKeys.RemoveKey(db, k)
		deleted++
	}
	if deleted > 0 {
		db.addAof(ToCmdLine2("del", args...))
	}
	return CreateStrResult(COk, OkStr)
}

func Keys(db *SaveDBTables, args []string) Result {
	pattern := strings.ReplaceAll(args[0], "*", ".*")
	var matchingKeys []string
	re := regexp.MustCompile(pattern)
	iter := db.AllKeys.keys.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		key := string(iter.Item().key)
		if re.MatchString(key) {
			matchingKeys = append(matchingKeys, key)
		}
	}
	iter.Release()
	res := strings.Join(matchingKeys, ",")
	return CreateStrResult(COk, res)
}

func Exists(db *SaveDBTables, args []string) Result {
	_, ok := db.Data.GetWithLock(args[0])
	if ok {
		return CreateStrResult(COk, "1")
	} else {
		return CreateStrResult(COk, "0")
	}
}

func findMatchingKeys(inputMap map[string]string, pattern string) []string {
	pattern = strings.ReplaceAll(pattern, "*", ".*")
	var matchingKeys []string
	re := regexp.MustCompile(pattern)

	for key := range inputMap {
		if re.MatchString(key) {
			matchingKeys = append(matchingKeys, key)
		}
	}

	return matchingKeys
}
func (a *AllKeys) PutKey(key string, keyType byte) {
	ki := &keyItem{
		key:     StringToBytes(key),
		saveObj: NewSaveObject(&key, keyType),
	}
	a.keys.Set(ki)
}

func (a *AllKeys) RemoveKey(db *SaveDBTables, key string) {
	ki := &keyItem{
		key: StringToBytes(key),
	}
	a.keys.Delete(ki)
	delete(db.Expires, key)
}

// key缓存命中
func (a *AllKeys) ActivateKey(key string) {
	ki := &keyItem{
		key: StringToBytes(key),
	}
	value, ok := a.keys.Get(ki)
	if !ok {
		return
	}
	//lfu衰减
	updateLFU(value.saveObj)
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
