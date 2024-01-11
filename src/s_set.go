package src

import (
	"fmt"
	"strconv"
	"strings"
)

// Set 表的实现直接使用go的map,在此之前需要了解go中的map基本机制
// 1、go中的map由多个bucket组成，每个bucket分为三个部分，tophash区域、keys区域、values区域，都是由内存连续的数组组成
// 2、扩容机制类似于redis的渐进式rehash，map的LoadFactor是6.5
// 3、如果 key 或 value 的数据长度大于一定数值(128)，那么运行时不会在 bucket 中直接存储数据，而是会存储 key 或 value 数据的指针。
// 4、go中的hashcode是吧key的hashcode一分为二，其中低位区的值用于选定 bucket，高位区的值用于在某个 bucket 中确定 key 的位置

type Set struct {
	M map[string]*struct{}
}

func NewSet() *Set {
	s := &Set{}
	s.M = make(map[string]*struct{})
	return s
}

func (db *SaveDBTables) GetOrCreateSet(key string) (*Set, error) {
	val, ok := db.Data.GetWithLock(key)
	if !ok {
		val = NewSet()
		db.AllKeys.PutKey(key, TypeSet)
		db.Data.PutWithLock(key, val)
		return val.(*Set), nil
	}
	if _, ok := val.(*Set); !ok {
		return nil, fmt.Errorf("type conversion error")
	}
	return val.(*Set), nil
}

func (db *SaveDBTables) GetSet(key string) (*Set, error) {
	val, ok := db.Data.GetWithLock(key)
	if !ok {
		return nil, nil
	}
	if _, ok := val.(*Set); !ok {
		return nil, fmt.Errorf("type conversion error")
	}
	db.AllKeys.ActivateKey(key)
	return val.(*Set), nil
}

func SAdd(db *SaveDBTables, args []string) Result {
	key := args[0]
	set, err := db.GetOrCreateSet(key)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	for _, value := range args[1:] {
		set.M[value] = &struct{}{}
	}

	return CreateStrResult(C_OK, strconv.Itoa(len(args[1:])))
}

func SMove(db *SaveDBTables, args []string) Result {
	key := args[0]
	set, err := db.GetSet(key)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	if set == nil {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	values := args[1:]
	if len(values) == 0 || values[0] == "" {
		return CreateStrResult(C_ERR, "value is null")
	}

	for _, value := range values {
		delete(set.M, value)
	}
	return CreateStrResult(C_OK, strconv.Itoa(len(values)))

}

func SHasKey(db *SaveDBTables, args []string) Result {
	key := args[0]
	v, err := db.GetSet(key)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	if v != nil {
		return CreateStrResult(C_OK, "-1")
	}
	return CreateStrResult(C_OK, "1")
}

func SPop(db *SaveDBTables, args []string) Result {
	key := args[0]
	res := SHasKey(db, args)
	if res.Status != 1 {
		return res
	}
	if string(res.Res) != "1" {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	v, _ := db.GetSet(key)

	if v != nil {
		return CreateStrResult(C_OK, "-1")
	}
	for val, _ := range v.M {
		delete(v.M, val)
		return CreateStrResult(C_OK, val)
	}

	return CreateStrResult(C_ERR, "value inexistence")
}

func SCard(db *SaveDBTables, args []string) Result {
	key := args[0]
	res := SHasKey(db, args)
	if res.Status != 1 {
		return res
	}
	if string(res.Res) != "1" {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	v, _ := db.GetSet(key)

	return CreateStrResult(C_OK, strconv.Itoa(len(v.M)))
}

func SDiff(db *SaveDBTables, args []string) Result {
	if (SHasKey(db, args[:1]).Status != C_OK) || (SHasKey(db, args[1:]).Status != C_OK) {
		return CreateStrResult(C_ERR, "set not exist")
	}
	records := make([]string, 0)
	key1 := args[0]
	key2 := args[1]
	set, err := db.GetSet(key1)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	if set == nil {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	set2, err := db.GetSet(key2)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	if set2 == nil {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	for v, _ := range set.M {
		if _, ok := set2.M[v]; !ok {
			records = append(records, v)
		}
	}
	result := strings.Join(records, ",")
	return CreateStrResult(C_OK, result)
}

func SInter(db *SaveDBTables, args []string) Result {
	if SHasKey(db, args[:1]).Status != C_OK || SHasKey(db, args[1:]).Status != C_OK {
		return CreateStrResult(C_ERR, "set not exist")
	}
	key1 := args[0]
	key2 := args[1]
	set, err := db.GetSet(key1)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	if set == nil {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	set2, err := db.GetSet(key2)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	if set2 == nil {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	values := make([]string, 0)
	for v, _ := range set.M {
		if _, ok := set2.M[v]; ok {
			values = append(values, v)
		}
	}
	result := strings.Join(values, ", ")
	return CreateStrResult(C_OK, result)
}

func SIsMember(db *SaveDBTables, args []string) Result {
	key := args[0]
	set, err := db.GetSet(key)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	if set == nil {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	value := args[1]
	if _, ok := set.M[value]; ok {
		return CreateResult(C_OK, nil)
	}

	return CreateStrResult(C_ERR, "set not exist")
}

func SAreMembers(db *SaveDBTables, args []string) Result {
	key := args[0]
	set, err := db.GetSet(key)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	if set == nil {
		return CreateStrResult(C_ERR, "key inexistence")
	}

	values := args[1:]
	for _, value := range values {
		if _, ok := set.M[value]; !ok {
			return CreateStrResult(C_ERR, "set not exist")
		}
	}
	return CreateResult(C_OK, nil)
}

func SMembers(db *SaveDBTables, args []string) Result {
	key := args[0]
	set, err := db.GetSet(key)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	if set == nil {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	records := make([]string, 0)
	for k, _ := range set.M {
		records = append(records, k)
	}
	result := strings.Join(records, ",")
	return CreateStrResult(C_OK, result)
}

func SUnion(db *SaveDBTables, args []string) Result {
	if SHasKey(db, args[:1]).Status != C_OK || SHasKey(db, args[1:]).Status != C_OK {
		return CreateStrResult(C_ERR, "set not exist")
	}

	key1 := args[0]
	key2 := args[1]
	set, err := db.GetSet(key1)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	if set == nil {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	set2, err := db.GetSet(key2)
	if err != nil {
		return CreateStrResult(C_ERR, err.Error())
	}
	if set2 == nil {
		return CreateStrResult(C_ERR, "key inexistence")
	}
	record1s := make([]string, 0)
	for k, _ := range set.M {
		record1s = append(record1s, k)
	}
	record2s := make([]string, 0)
	for v, _ := range set2.M {
		if _, ok := set.M[v]; !ok {
			record2s = append(record2s, v)
		}
	}
	result := strings.Join(record2s, ",")
	return CreateStrResult(C_OK, result)
}
