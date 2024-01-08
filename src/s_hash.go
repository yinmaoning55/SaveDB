package src

import (
	"strconv"
	"strings"
)

// Hash 基本上和set一样
type Hash struct {
	M map[string]map[string]*string
}

func NewHash() *Hash {
	h := &Hash{}
	h.M = make(map[string]map[string]*string)
	return h
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

	hash, ok := db.Hash.M[key]
	if !ok {
		db.Hash.M[key] = make(map[string]*string)
		hash = db.Hash.M[key]
	}
	for i, value := range fields {
		hash[value] = &values[i]
	}
	return CreateResult(C_OK, []byte(strconv.Itoa(len(values))))
}

func HMGet(db *saveDBTables, args []string) Result {
	key := args[0]
	key2 := args[1]
	// get entity
	dict, ok := db.Hash.M[key]
	if ok {
		return CreateStrResult(C_ERR, "key not exist")
	}
	value, ok := dict[key2]
	if !ok {
		return CreateStrResult(C_ERR, "key2 not exist")
	}

	return CreateStrResult(C_OK, *value)
}

func HDel(db *saveDBTables, args []string) Result {
	hash, ok := db.Hash.M[args[0]]
	if !ok {
		return CreateStrResult(C_ERR, "key inexistence")
	}

	if len(args[1:]) == 0 || args[0] == "" {
		return CreateStrResult(C_ERR, "key inexistence")
	}

	for _, value := range args[1:] {
		delete(hash, value)
	}

	return CreateResult(C_OK, []byte(strconv.Itoa(len(args[1:]))))
}

func HExistsToFiled(db *saveDBTables, args []string) Result {
	key1 := args[0]
	key2 := args[1]
	if v, ok := db.Hash.M[key1]; ok {
		if _, ok := v[key2]; ok {
			return CreateResult(C_OK, nil)
		}
	}
	return CreateStrResult(C_ERR, "key inexistence")
}
func HExists(db *saveDBTables, args []string) Result {
	if _, ok := db.Hash.M[args[0]]; ok {
		return CreateResult(C_OK, nil)
	}
	return CreateStrResult(C_ERR, "key inexistence")
}

func HCard(db *saveDBTables, args []string) Result {
	if HExists(db, args).Status != C_OK {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	return CreateResult(C_OK, []byte(strconv.Itoa(len(db.Hash.M[args[0]]))))
}

func HGetAll(db *saveDBTables, args []string) Result {
	if _, ok := db.Hash.M[args[0]]; !ok {
		return CreateStrResult(C_ERR, "hash not exist")
	}

	records := make([]string, 0)
	var builder strings.Builder
	var index = 0
	var size = len(db.Hash.M[args[0]])
	for k, record := range db.Hash.M[args[0]] {
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
func DelHash(db *saveDBTables, key string) {
	delete(db.Hash.M, key)
	db.AllKeys.RemoveKey(db, key)
}
