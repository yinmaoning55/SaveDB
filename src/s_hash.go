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
func (db *saveDBTables) GetOrCreateHash(key string) (*Hash, error) {
	val, ok := db.Data.GetWithLock(key)
	if !ok {
		val = NewHash()
		db.Data.PutWithLock(key, val)
		db.AllKeys.PutKey(key, TypeHash)
		return nil, fmt.Errorf("type conversion error")
	}
	if _, ok := val.(*Hash); !ok {
		return nil, fmt.Errorf("")
	}
	return val.(*Hash), nil
}
func (db *saveDBTables) GetHash(key string) (*Hash, error) {
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

	hash, err := db.GetOrCreateHash(key)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	for i, value := range fields {
		hash.M[value] = &values[i]
	}
	return CreateResult(C_OK, []byte(strconv.Itoa(len(values))))
}

func HMGet(db *saveDBTables, args []string) Result {
	key := args[0]
	key2 := args[1]
	// get entity
	dict, err := db.GetHash(key)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
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
	hash, err := db.GetHash(key)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
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

func HExists(db *saveDBTables, args []string) Result {
	key1 := args[0]
	key2 := args[1]
	v, err := db.GetHash(key1)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	if v != nil {
		if _, ok := v.M[key2]; ok {
			return CreateResult(C_OK, nil)
		}
	}
	return CreateStrResult(C_ERR, "key inexistence")
}

func HCard(db *saveDBTables, args []string) Result {
	if string(Exists(db, args).Res) != "1" {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	key := args[0]
	v, err := db.GetHash(key)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	return CreateResult(C_OK, []byte(strconv.Itoa(len(v.M))))
}

func HGetAll(db *saveDBTables, args []string) Result {
	key := args[0]
	v, err := db.GetHash(key)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	if v != nil {
		return CreateStrResult(C_ERR, "hash not exist")
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
	return CreateStrResult(C_OK, builder.String())
}
