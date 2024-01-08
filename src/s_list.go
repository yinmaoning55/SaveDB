package src

import (
	"fmt"
	"strconv"
	"strings"
)

type List struct {
	L map[string]*QuickList
}

func NewList() *List {
	l := &List{}
	l.L = make(map[string]*QuickList)
	return l
}

func LLen(db *saveDBTables, args []string) Result {
	key := args[0]

	list, ok := db.List.L[key]
	if !ok {
		return CreateStrResult(C_ERR, "list not exist")
	}
	size := list.Len()
	return CreateStrResult(C_OK, strconv.Itoa(size))
}

func LPop(db *saveDBTables, args []string) Result {
	key := args[0]

	// get data
	list, ok := db.List.L[key]
	if !ok {
		return CreateStrResult(C_ERR, "list not exist")
	}
	val, _ := list.Remove(0).(string)
	if list.Len() == 0 {
		DelList(db, key)
	}
	return CreateStrResult(C_OK, val)
}

func LPush(db *saveDBTables, args []string) Result {
	key := args[0]
	values := args[1:]

	list, ok := db.List.L[key]
	if !ok {
		db.List.L[key] = NewQuickList()
	}
	// insert
	for _, value := range values {
		list.Insert(0, value)
	}
	return CreateStrResult(C_OK, strconv.Itoa(list.Len()))
}

func LPushX(db *saveDBTables, args []string) Result {
	key := args[0]
	values := args[1:]

	// get or init entity
	list, ok := db.List.L[key]
	if !ok {
		db.List.L[key] = NewQuickList()
	}
	// insert
	for _, value := range values {
		list.Insert(0, value)
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
	list, ok := db.L[key]
	if !ok {
		return CreateStrResult(C_ERR, "list not exist")
	}

	// compute index
	size := list.Len() // assert: size > 0
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
	slice := list.Range(start, stop)
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

	list, ok := db.L[key]
	if !ok {
		return CreateStrResult(C_ERR, "list not exist")
	}
	if list == nil {
		return CreateStrResult(C_OK, "0")
	}

	var removed int
	if count == 0 {
		removed = list.RemoveAllByVal(func(a interface{}) bool {
			return Equals(a, value)
		})
	} else if count > 0 {
		removed = list.RemoveByVal(func(a interface{}) bool {
			return Equals(a, value)
		}, count)
	} else {
		removed = list.ReverseRemoveByVal(func(a interface{}) bool {
			return Equals(a, value)
		}, -count)
	}

	if list.Len() == 0 {
		DelList(db, key)
	}
	if removed > 0 {
		//aof
	}

	return CreateStrResult(C_OK, strconv.Itoa(removed))
}
func DelList(db *saveDBTables, key string) {
	delete(db.List.L, key)
	db.AllKeys.RemoveKey(db, key)
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
	list, ok := db.L[key]
	if !ok {
		return CreateStrResult(C_ERR, "list not exist")
	}
	if list == nil {
		return CreateStrResult(C_ERR, "ERR no such key")
	}

	size := list.Len() // assert: size > 0
	if index < -1*size {
		return CreateStrResult(C_ERR, "ERR index out of range")
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return CreateStrResult(C_ERR, "ERR index out of range")
	}

	list.Set(index, value)
	db.AllKeys.PutKey(key, TypeList)
	return CreateResult(C_OK, nil)
}

func RPop(db *saveDBTables, args []string) Result {
	// parse args
	key := args[0]

	// get data
	list, ok := db.L[key]
	if !ok {
		return CreateStrResult(C_ERR, "list not exist")
	}
	if list == nil {
		return CreateStrResult(C_ERR, "ERR no such key")
	}

	val, _ := list.RemoveLast().(string)
	if list.Len() == 0 {
		DelList(db, key)
	}
	//aof

	return CreateStrResult(C_OK, val)
}

func RPopLPush(db *saveDBTables, args []string) Result {
	sourceKey := args[0]
	destKey := args[1]

	// get source entity
	list, ok := db.L[sourceKey]
	if !ok {
		return CreateStrResult(C_ERR, "list not exist")
	}
	if list == nil {
		return CreateStrResult(C_ERR, "ERR no such key")
	}

	// get dest entity
	destList, ok := db.L[destKey]
	if !ok {
		return CreateStrResult(C_ERR, "list not exist")
	}
	if destList == nil {
		return CreateStrResult(C_ERR, "list not exist")
	}
	// pop and push
	val, _ := list.RemoveLast().(string)
	destList.Insert(0, val)

	if list.Len() == 0 {
		DelList(db, sourceKey)
	}

	return CreateStrResult(C_OK, val)
}

func RPush(db *saveDBTables, args []string) Result {
	// parse args
	key := args[0]
	values := args[1:]

	// get or init entity
	list, ok := db.L[key]
	if !ok {
		return CreateStrResult(C_ERR, "list not exist")
	}

	// put list
	for _, value := range values {
		list.Add(value)
	}
	return CreateStrResult(C_OK, strconv.Itoa(list.Len()))
}

func RPushX(db *saveDBTables, args []string) Result {
	if len(args) < 2 {
		return CreateStrResult(C_ERR, "ERR wrong number of arguments for 'rpush' command")
	}
	key := args[0]
	values := args[1:]

	// get or init entity
	list, ok := db.L[key]
	if !ok {
		return CreateStrResult(C_ERR, "list not exist")
	}
	if list == nil {
		return CreateStrResult(C_ERR, "list is null")
	}

	// put list
	for _, value := range values {
		list.Add(value)
	}
	return CreateStrResult(C_OK, strconv.Itoa(list.Len()))
}

func LTrim(db *saveDBTables, args []string) Result {
	n := len(args)
	if n != 3 {
		return CreateStrResult(C_ERR, fmt.Sprintf("ERR wrong number of arguments (given %d, expected 3)", n))
	}
	key := string(args[0])
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return CreateStrResult(C_ERR, "ERR value is not an integer or out of range")
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		return CreateStrResult(C_ERR, "ERR value is not an integer or out of range")
	}
	list, ok := db.L[key]
	if !ok {
		return CreateStrResult(C_ERR, "list not exist")
	}
	if list == nil {
		return CreateStrResult(C_ERR, "list is null")
	}
	length := list.Len()
	if start < 0 {
		start += length
	}
	if end < 0 {
		end += length
	}

	leftCount := start
	rightCount := length - end - 1

	for i := 0; i < leftCount && list.Len() > 0; i++ {
		list.Remove(0)
	}
	for i := 0; i < rightCount && list.Len() > 0; i++ {
		list.RemoveLast()
	}
	return CreateResult(C_OK, nil)
}

func LInsert(db *saveDBTables, args []string) Result {
	n := len(args)
	if n != 4 {
		return CreateStrResult(C_ERR, "ERR wrong number of arguments for 'linsert' command")
	}
	key := args[0]
	list, ok := db.L[key]
	if !ok {
		return CreateStrResult(C_ERR, "list not exist")
	}
	if list == nil {
		return CreateStrResult(C_ERR, "list is null")
	}

	dir := strings.ToLower(string(args[1]))
	if dir != "before" && dir != "after" {
		return CreateStrResult(C_ERR, "ERR syntax error")
	}

	pivot := args[2]
	index := -1
	list.ForEach(func(i int, v interface{}) bool {
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
		list.Insert(index, val)
	} else {
		list.Insert(index+1, val)
	}
	return CreateStrResult(C_OK, strconv.Itoa(list.Len()))
}
