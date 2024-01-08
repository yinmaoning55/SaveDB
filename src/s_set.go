package src

import (
	"errors"
	"strconv"
	"strings"
)

// Set 表的实现直接使用go的map,在此之前需要了解go中的map基本机制
// 1、go中的map由多个bucket组成，每个bucket分为三个部分，tophash区域、keys区域、values区域，都是由内存连续的数组组成
// 2、扩容机制类似于redis的渐进式rehash，map的LoadFactor是6.5
// 3、如果 key 或 value 的数据长度大于一定数值(128)，那么运行时不会在 bucket 中直接存储数据，而是会存储 key 或 value 数据的指针。
// 4、go中的hashcode是吧key的hashcode一分为二，其中低位区的值用于选定 bucket，高位区的值用于在某个 bucket 中确定 key 的位置
var (
	ErrSetNotExist = errors.New("set not exist")

	ErrMemberEmpty = errors.New("item empty")
)

type Set struct {
	M map[string]map[string]*struct{}
}

func NewSet() *Set {
	s := &Set{}
	s.M = make(map[string]map[string]*struct{})
	return s
}

func SAdd(db *saveDBTables, args []string) Result {
	key := args[0]
	set, ok := db.Set.M[key]
	if !ok {
		db.Set.M[key] = make(map[string]*struct{})
		set = db.Set.M[key]
	}
	for _, value := range args[1:] {
		set[value] = &struct{}{}
	}
	return CreateStrResult(C_OK, strconv.Itoa(len(args[1:])))
}

func SMove(db *saveDBTables, args []string) Result {
	key := args[0]
	set, ok := db.Set.M[key]
	if !ok {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	values := args[1:]
	if len(values) == 0 || values[0] == "" {
		return CreateStrResult(C_ERR, "value is null")
	}

	for _, value := range values {
		delete(set, value)
	}
	return CreateStrResult(C_OK, strconv.Itoa(len(values)))

}

func SHasKey(db *saveDBTables, args []string) Result {
	key := args[0]
	if _, ok := db.Set.M[key]; ok {
		return CreateResult(C_OK, nil)
	}
	return CreateResult(C_ERR, nil)
}

func SPop(db *saveDBTables, args []string) Result {
	key := args[0]
	if SHasKey(db, args).Status != C_OK {
		return CreateStrResult(C_ERR, "key inexistence")
	}

	for v, _ := range db.Set.M[key] {
		delete(db.Set.M[key], v)
		return CreateStrResult(C_OK, v)
	}

	return CreateStrResult(C_ERR, "value inexistence")
}

func SCard(db *saveDBTables, args []string) Result {
	key := args[0]
	if SHasKey(db, args).Status != C_OK {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	return CreateStrResult(C_OK, strconv.Itoa(len(db.Set.M[key])))
}

func SDiff(db *saveDBTables, args []string) Result {
	if (SHasKey(db, args[:1]).Status != C_OK) || (SHasKey(db, args[1:]).Status != C_OK) {
		return CreateStrResult(C_ERR, "set not exist")
	}
	records := make([]string, 0)
	key1 := args[0]
	key2 := args[1]
	for v, _ := range db.Set.M[key1] {
		if _, ok := db.Set.M[key2][v]; !ok {
			records = append(records, v)
		}
	}
	result := strings.Join(records, ",")
	return CreateStrResult(C_OK, result)
}

func SInter(db *saveDBTables, args []string) Result {
	if SHasKey(db, args[:1]).Status != C_OK || SHasKey(db, args[1:]).Status != C_OK {
		return CreateStrResult(C_ERR, "set not exist")
	}
	key1 := args[0]
	key2 := args[1]
	values := make([]string, 0)
	for v, _ := range db.Hash.M[key1] {
		if _, ok := db.Hash.M[key2][v]; ok {
			values = append(values, v)
		}
	}
	result := strings.Join(values, ", ")
	return CreateStrResult(C_OK, result)
}
func DelSet(db *saveDBTables, key string) {
	delete(db.Set.M, key)
	db.AllKeys.RemoveKey(db, key)
}
func SIsMember(db *saveDBTables, args []string) Result {
	key := args[0]
	if _, ok := db.Set.M[key]; !ok {
		return CreateStrResult(C_ERR, "set not exist")
	}
	value := args[1]
	if _, ok := db.Set.M[key][value]; ok {
		return CreateResult(C_OK, nil)
	}

	return CreateStrResult(C_ERR, "set not exist")
}

func SAreMembers(db *saveDBTables, args []string) Result {
	key := args[0]
	if _, ok := db.Set.M[key]; !ok {
		return CreateStrResult(C_ERR, "set not exist")
	}
	values := args[1:]
	for _, value := range values {
		if _, ok := db.Set.M[key][value]; !ok {
			return CreateStrResult(C_ERR, "set not exist")
		}
	}
	return CreateResult(C_OK, nil)
}

func SMembers(db *saveDBTables, args []string) Result {
	key := args[0]
	if _, ok := db.Set.M[key]; !ok {
		return CreateStrResult(C_ERR, "set not exist")
	}
	records := make([]string, 0)
	for k, _ := range db.Set.M[key] {
		records = append(records, k)
	}
	result := strings.Join(records, ",")
	return CreateStrResult(C_OK, result)
}

func SUnion(db *saveDBTables, args []string) Result {
	if SHasKey(db, args[:1]).Status != C_OK || SHasKey(db, args[1:]).Status != C_OK {
		return CreateStrResult(C_ERR, "set not exist")
	}

	key1 := args[0]
	key2 := args[1]
	record1s := make([]string, 0)
	for k, _ := range db.Set.M[key1] {
		record1s = append(record1s, k)
	}
	record2s := make([]string, 0)
	for v, _ := range db.Set.M[key2] {
		if _, ok := db.Set.M[key1][v]; !ok {
			record2s = append(record2s, v)
		}
	}
	result := strings.Join(record2s, ",")
	return CreateStrResult(C_OK, result)
}
