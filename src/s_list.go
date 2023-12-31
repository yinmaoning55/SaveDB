package src

import (
	"fmt"
	"savedb/src/data"
	"strconv"
	"strings"
)

type List struct {
	L *data.QuickList
}

func NewList() *List {
	l := &List{}
	l.L = data.NewQuickList()
	return l
}
func (db *saveDBTables) GetOrCreateList(key string) *List {
	val, ok := db.Data.Get(key)
	if !ok {
		val = NewList()
		db.AllKeys.PutKey(key, TypeList)
		db.Data.Put(key, val)
		return val.(*List)
	}
	return val.(*List)
}

func (db *saveDBTables) GetList(key string) *List {
	val, ok := db.Data.Get(key)
	if !ok {
		return nil
	}
	db.AllKeys.ActivateKey(key)
	return val.(*List)
}
func LLen(db *saveDBTables, args []string) Result {
	key := args[0]

	list := db.GetList(key)
	if list == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}
	size := list.L.Len()
	return CreateStrResult(C_OK, strconv.Itoa(size))
}

func LPop(db *saveDBTables, args []string) Result {
	key := args[0]

	// get data
	list := db.GetList(key)
	if list == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}
	val, _ := list.L.Remove(0).(string)
	if list.L.Len() == 0 {
		Delete(db, args)
	}
	return CreateStrResult(C_OK, val)
}

func LPush(db *saveDBTables, args []string) Result {
	key := args[0]
	values := args[1:]

	list := db.GetOrCreateList(key)
	// insert
	for _, value := range values {
		list.L.Insert(0, value)
	}
	return CreateStrResult(C_OK, strconv.Itoa(list.L.Len()))
}

func LPushX(db *saveDBTables, args []string) Result {
	key := args[0]
	values := args[1:]

	// get or init entity
	list := db.GetList(key)
	if list == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}
	// insert
	for _, value := range values {
		list.L.Insert(0, value)
	}
	return CreateStrResult(C_OK, strconv.Itoa(len(values)))
}

func LRange(db *saveDBTables, args []string) Result {
	// parse args
	key := args[0]
	start64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return CreateStrResult(C_ERR, "ERR value is not an integer or out of range")
	}
	start := int(start64)
	stop64, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return CreateStrResult(C_ERR, "args tar")
	}
	stop := int(stop64)

	// get data
	list := db.GetList(key)
	if list == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}

	// compute index
	size := list.L.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return CreateResult(C_OK, nil)
	}
	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}
	if stop < start {
		stop = start
	}

	// assert: start in [0, size - 1], stop in [start, size]
	slice := list.L.Range(start, stop)
	result := make([]string, len(slice))

	for i, raw := range slice {
		bytes, _ := raw.(string)
		result[i] = bytes
	}
	r := strings.Join(result, ",")
	return CreateStrResult(C_OK, r)
}

func LRem(db *saveDBTables, args []string) Result {
	// parse args
	key := args[0]
	count64, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return CreateStrResult(C_ERR, "ERR value is not an integer or out of range")
	}
	count := int(count64)
	value := args[2]

	list := db.GetList(key)
	if list == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}

	var removed int
	if count == 0 {
		removed = list.L.RemoveAllByVal(func(a interface{}) bool {
			return Equals(a, value)
		})
	} else if count > 0 {
		removed = list.L.RemoveByVal(func(a interface{}) bool {
			return Equals(a, value)
		}, count)
	} else {
		removed = list.L.ReverseRemoveByVal(func(a interface{}) bool {
			return Equals(a, value)
		}, -count)
	}

	if list.L.Len() == 0 {
		Delete(db, args)
	}
	if removed > 0 {
		//aof
	}

	return CreateStrResult(C_OK, strconv.Itoa(removed))
}

func LSet(db *saveDBTables, args []string) Result {
	// parse args
	key := args[0]
	index64, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return CreateStrResult(C_ERR, "ERR value is not an integer or out of range")
	}

	index := int(index64)
	value := args[2]
	// get data
	list := db.GetList(key)
	if list == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}
	if list == nil {
		return CreateStrResult(C_ERR, "ERR no such key")
	}

	size := list.L.Len() // assert: size > 0
	if index < -1*size {
		return CreateStrResult(C_ERR, "ERR index out of range")
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return CreateStrResult(C_ERR, "ERR index out of range")
	}

	list.L.Set(index, value)
	db.AllKeys.PutKey(key, TypeList)
	return CreateResult(C_OK, nil)
}

func RPop(db *saveDBTables, args []string) Result {
	// parse args
	key := args[0]

	// get data
	list := db.GetList(key)
	if list == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}
	if list == nil {
		return CreateStrResult(C_ERR, "ERR no such key")
	}

	val, _ := list.L.RemoveLast().(string)
	if list.L.Len() == 0 {
		Delete(db, args)
	}
	//aof

	return CreateStrResult(C_OK, val)
}

func RPopLPush(db *saveDBTables, args []string) Result {
	sourceKey := args[0]
	destKey := args[1]

	// get source entity
	list := db.GetList(sourceKey)
	if list == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}

	// get dest entity
	destList := db.GetList(destKey)
	if destList == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}
	if destList == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}
	// pop and push
	val, _ := list.L.RemoveLast().(string)
	destList.L.Insert(0, val)

	if list.L.Len() == 0 {
		Delete(db, args)
	}

	return CreateStrResult(C_OK, val)
}

func RPush(db *saveDBTables, args []string) Result {
	// parse args
	key := args[0]
	values := args[1:]

	// get or init entity
	list := db.GetList(key)
	if list == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}
	// put list
	for _, value := range values {
		list.L.Add(value)
	}
	return CreateStrResult(C_OK, strconv.Itoa(list.L.Len()))
}

func RPushX(db *saveDBTables, args []string) Result {
	if len(args) < 2 {
		return CreateStrResult(C_ERR, "ERR wrong number of arguments for 'rpush' command")
	}
	key := args[0]
	values := args[1:]

	// get or init entity
	list := db.GetList(key)
	if list == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}

	// put list
	for _, value := range values {
		list.L.Add(value)
	}
	return CreateStrResult(C_OK, strconv.Itoa(list.L.Len()))
}

func LTrim(db *saveDBTables, args []string) Result {
	n := len(args)
	if n != 3 {
		return CreateStrResult(C_ERR, fmt.Sprintf("ERR wrong number of arguments (given %d, expected 3)", n))
	}
	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return CreateStrResult(C_ERR, "ERR value is not an integer or out of range")
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		return CreateStrResult(C_ERR, "ERR value is not an integer or out of range")
	}
	list := db.GetList(key)
	if list == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}
	if list == nil {
		return CreateStrResult(C_ERR, "list is null")
	}
	length := list.L.Len()
	if start < 0 {
		start += length
	}
	if end < 0 {
		end += length
	}

	leftCount := start
	rightCount := length - end - 1

	for i := 0; i < leftCount && list.L.Len() > 0; i++ {
		list.L.Remove(0)
	}
	for i := 0; i < rightCount && list.L.Len() > 0; i++ {
		list.L.RemoveLast()
	}
	return CreateResult(C_OK, nil)
}

func LInsert(db *saveDBTables, args []string) Result {
	n := len(args)
	if n != 4 {
		return CreateStrResult(C_ERR, "ERR wrong number of arguments for 'linsert' command")
	}
	key := args[0]
	list := db.GetList(key)
	if list == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}

	dir := strings.ToLower(string(args[1]))
	if dir != "before" && dir != "after" {
		return CreateStrResult(C_ERR, "ERR syntax error")
	}

	pivot := args[2]
	index := -1
	list.L.ForEach(func(i int, v interface{}) bool {
		if string(v.([]byte)) == pivot {
			index = i
			return false
		}
		return true
	})
	if index == -1 {
		return CreateStrResult(C_ERR, "index = -1")
	}

	val := args[3]
	if dir == "before" {
		list.L.Insert(index, val)
	} else {
		list.L.Insert(index+1, val)
	}
	return CreateStrResult(C_OK, strconv.Itoa(list.L.Len()))
}
