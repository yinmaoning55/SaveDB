package src

import (
	"strconv"
	"strings"
)

// Hash 基本上和set一样
type Hash struct {
	M map[string]*string
}

func NewHash() *Hash {
	h := &Hash{}
	h.M = make(map[string]*string)
	return h
}
func (db *saveDBTables) GetOrCreateHash(key string) *Hash {
	val, ok := db.Data.Get(key)
	if !ok {
		val = NewHash()
		db.Data.Put(key, val)
		db.AllKeys.PutKey(key, TypeHash)
		return val.(*Hash)
	}
	return val.(*Hash)
}
func (db *saveDBTables) GetHash(key string) *Hash {
	val, ok := db.Data.Get(key)
	if !ok {
		return nil
	}
	db.AllKeys.ActivateKey(key)
	return val.(*Hash)
}
func HmSet(db *saveDBTables, args []string) Result {
	if len(args)%2 != 1 {
		return CreateStrResult(C_ERR, "args number error")
	}
	key := args[0]
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	values := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = args[2*i+1]
		values[i] = args[2*i+2]
	}

	hash := db.GetOrCreateHash(key)
	for i, value := range fields {
		hash.M[value] = &values[i]
	}
	return CreateResult(C_OK, []byte(strconv.Itoa(len(values))))
}

func HMGet(db *saveDBTables, args []string) Result {
	key := args[0]
	key2 := args[1]
	// get entity
	dict := db.GetHash(key)
	if dict == nil {
		return CreateStrResult(C_ERR, "key not exist")
	}
	value, ok := dict.M[key2]
	if !ok {
		return CreateStrResult(C_ERR, "key2 not exist")
	}

	return CreateStrResult(C_OK, *value)
}

func HDel(db *saveDBTables, args []string) Result {
	key := args[0]
	hash := db.GetHash(key)
	if hash == nil {
		return CreateStrResult(C_ERR, "key inexistence")
	}

	if len(args[1:]) == 0 || key == "" {
		return CreateStrResult(C_ERR, "key inexistence")
	}

	for _, value := range args[1:] {
		delete(hash.M, value)
	}

	return CreateResult(C_OK, []byte(strconv.Itoa(len(args[1:]))))
}

func HExistsToFiled(db *saveDBTables, args []string) Result {
	key1 := args[0]
	key2 := args[1]
	if v := db.GetHash(key1); v != nil {
		if _, ok := v.M[key2]; ok {
			return CreateResult(C_OK, nil)
		}
	}
	return CreateStrResult(C_ERR, "key inexistence")
}
func HExists(db *saveDBTables, args []string) Result {
	key := args[0]
	if v := db.GetHash(key); v != nil {
		return CreateResult(C_OK, nil)
	}
	return CreateStrResult(C_ERR, "key inexistence")
}

func HCard(db *saveDBTables, args []string) Result {
	if HExists(db, args).Status != C_OK {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	key := args[0]
	return CreateResult(C_OK, []byte(strconv.Itoa(len(db.GetHash(key).M))))
}

func HGetAll(db *saveDBTables, args []string) Result {
	key := args[0]
	if v := db.GetHash(key); v != nil {
		return CreateStrResult(C_ERR, "hash not exist")
	}

	records := make([]string, 0)
	var builder strings.Builder
	var index = 0
	var size = len(db.GetHash(key).M)
	for k, record := range db.GetHash(key).M {
		records = append(records, *record)
		builder.WriteString(k)
		builder.WriteString("=")
		builder.WriteString(*record)
		if index < size-1 {
			builder.WriteString(",")
		}
		index++
	}
	return CreateStrResult(C_OK, builder.String())
}
