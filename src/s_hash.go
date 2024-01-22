package src

import (
	"fmt"
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
func (db *SaveDBTables) GetOrCreateHash(key string) (*Hash, error) {
	val, ok := db.Data.GetWithLock(key)
	if !ok {
		val = NewHash()
		db.Data.PutWithLock(key, val)
		db.AllKeys.PutKey(key, TypeHash)
		return val.(*Hash), nil
	}
	if _, ok := val.(*Hash); !ok {
		return nil, fmt.Errorf("")
	}
	return val.(*Hash), nil
}
func (db *SaveDBTables) GetHash(key string) (*Hash, error) {
	val, ok := db.Data.GetWithLock(key)
	if !ok {
		return nil, nil
	}
	if _, ok := val.(*Hash); !ok {
		return nil, fmt.Errorf("")
	}
	db.AllKeys.ActivateKey(key)
	return val.(*Hash), nil
}
func HmSet(db *SaveDBTables, args []string) Result {
	if len(args)%2 != 1 {
		return CreateStrResult(CErr, "args number error")
	}
	key := args[0]
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	values := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = args[2*i+1]
		values[i] = args[2*i+2]
	}

	hash, err := db.GetOrCreateHash(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	for i, value := range fields {
		hash.M[value] = &values[i]
	}
	db.addAof(ToCmdLine2("hmset", args...))
	return CreateResult(COk, []byte(strconv.Itoa(len(values))))
}

func HGet(db *SaveDBTables, args []string) Result {
	key := args[0]
	key2 := args[1]
	// get entity
	dict, err := db.GetHash(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if dict == nil {
		return CreateStrResult(CErr, "key not exist")
	}
	value, ok := dict.M[key2]
	if !ok {
		return CreateStrResult(CErr, "key2 not exist")
	}

	return CreateStrResult(COk, *value)
}

func HDel(db *SaveDBTables, args []string) Result {
	key := args[0]
	hash, err := db.GetHash(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if hash == nil {
		return CreateStrResult(CErr, "key inexistence")
	}

	if len(args[1:]) == 0 || key == "" {
		return CreateStrResult(CErr, "key inexistence")
	}

	for _, value := range args[1:] {
		delete(hash.M, value)
	}
	db.addAof(ToCmdLine2("hdel", args...))
	return CreateResult(COk, []byte(strconv.Itoa(len(args[1:]))))
}

func HExists(db *SaveDBTables, args []string) Result {
	key1 := args[0]
	key2 := args[1]
	v, err := db.GetHash(key1)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if v != nil {
		if _, ok := v.M[key2]; ok {
			return CreateResult(COk, nil)
		}
	}
	return CreateStrResult(CErr, "key inexistence")
}

func HCard(db *SaveDBTables, args []string) Result {
	if string(Exists(db, args).Res) != "1" {
		return CreateStrResult(CErr, "key inexistence")
	}
	key := args[0]
	v, err := db.GetHash(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	return CreateResult(COk, []byte(strconv.Itoa(len(v.M))))
}

func HGetAll(db *SaveDBTables, args []string) Result {
	key := args[0]
	v, err := db.GetHash(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if v == nil {
		return CreateStrResult(CErr, "hash not exist")
	}

	records := make([]string, 0)
	var builder strings.Builder
	var index = 0
	var size = len(v.M)
	for k, record := range v.M {
		records = append(records, *record)
		builder.WriteString(k)
		builder.WriteString("=")
		builder.WriteString(*record)
		if index < size-1 {
			builder.WriteString(",")
		}
		index++
	}
	return CreateStrResult(COk, builder.String())
}
