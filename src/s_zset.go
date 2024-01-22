package src

import (
	"fmt"
	"math"
	"savedb/src/data"
	"strconv"
	"strings"
)

type ZSet struct {
	Z *data.SortedSet
}

func NewZSet() *ZSet {
	z := &ZSet{}
	z.Z = data.MakeSortedSet()
	return z
}
func (db *SaveDBTables) GetOrCreateZSet(key string) (*ZSet, error) {
	val, ok := db.Data.GetWithLock(key)
	if !ok {
		val = NewZSet()
		db.AllKeys.PutKey(key, TypeZSet)
		db.Data.PutWithLock(key, val)
		return val.(*ZSet), nil
	}
	if _, ok := val.(*ZSet); !ok {
		return nil, fmt.Errorf("type conversion error")
	}
	return val.(*ZSet), nil
}

func (db *SaveDBTables) GetZSet(key string) (*ZSet, error) {
	val, ok := db.Data.GetWithLock(key)
	if !ok {
		return nil, nil
	}
	if _, ok := val.(*ZSet); !ok {
		return nil, fmt.Errorf("type conversion error")
	}
	db.AllKeys.ActivateKey(key)
	return val.(*ZSet), nil
}

func ZAdd(db *SaveDBTables, args []string) Result {
	if len(args)%2 != 1 {
		return CreateStrResult(CErr, "syntax err")
	}
	key := args[0]
	size := (len(args) - 1) / 2
	elements := make([]*data.Element, size)
	for i := 0; i < size; i++ {
		scoreValue := args[2*i+1]
		member := args[2*i+2]
		score, err := strconv.ParseFloat(string(scoreValue), 64)
		if err != nil {
			return CreateStrResult(CErr, "ERR value is not a valid float")
		}
		elements[i] = &data.Element{
			Member: member,
			Score:  score,
		}
	}

	// get or init entity
	sortedSet, err := db.GetOrCreateZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	i := 0
	for _, e := range elements {
		if sortedSet.Z.Add(e.Member, e.Score) {
			i++
		}
	}

	//persistence
	db.addAof(ToCmdLine2("zadd", args...))
	//添加全局key
	db.AllKeys.PutKey(key, TypeZSet)
	return CreateStrResult(COk, strconv.Itoa(i))
}

// execZScore gets score of a member in sortedset
func ZScore(db *SaveDBTables, args []string) Result {
	// parse args
	key := args[0]
	member := args[1]

	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	element, exists := sortedSet.Z.Get(member)
	if !exists {
		return CreateStrResult(CErr, "zSet key not exists")
	}
	value := strconv.FormatFloat(element.Score, 'f', -1, 64)
	return CreateStrResult(COk, value)
}

func ZRank(db *SaveDBTables, args []string) Result {
	// parse args
	key := args[0]
	member := args[1]

	// get entity
	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	rank := sortedSet.Z.GetRank(member, false)
	if rank < 0 {
		return CreateResult(COk, nil)
	}
	return CreateStrResult(COk, strconv.FormatInt(rank, 10))
}

func ZRevRank(db *SaveDBTables, args []string) Result {
	// parse args
	key := args[0]
	member := args[1]

	// get entity
	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	rank := sortedSet.Z.GetRank(member, true)
	if rank < 0 {
		return CreateResult(COk, nil)
	}
	return CreateStrResult(COk, strconv.FormatInt(rank, 10))
}

// execZCard gets number of members in sortedset
func ZCard(db *SaveDBTables, args []string) Result {
	// parse args
	key := args[0]

	// get entity
	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	return CreateStrResult(COk, strconv.FormatInt(sortedSet.Z.Len(), 10))
}

func ZRange(db *SaveDBTables, args []string) Result {
	// parse args
	if len(args) != 3 && len(args) != 4 {
		return CreateStrResult(CErr, "ERR wrong number of arguments for 'zrange' command")
	}
	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(args[3]) != "WITHSCORES" {
			return CreateStrResult(CErr, "syntax error")
		}
		withScores = true
	}
	key := args[0]
	start, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return CreateStrResult(CErr, "ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return CreateStrResult(CErr, "ERR value is not an integer or out of range")
	}
	return range0(db, key, start, stop, withScores, false)
}

// execZRevRange gets members in range, sort by score in descending order
func ZRevRange(db *SaveDBTables, args []string) Result {
	// parse args
	if len(args) != 3 && len(args) != 4 {
		return CreateStrResult(CErr, "ERR wrong number of arguments for 'zrevrange' command")
	}
	withScores := false
	if len(args) == 4 {
		if string(args[3]) != "WITHSCORES" {
			return CreateStrResult(CErr, "syntax error")
		}
		withScores = true
	}
	key := args[0]
	start, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return CreateStrResult(CErr, "ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return CreateStrResult(CErr, "ERR value is not an integer or out of range")
	}
	return range0(db, key, start, stop, withScores, true)
}

func range0(db *SaveDBTables, key string, start int64, stop int64, withScores bool, desc bool) Result {
	// get data
	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	// compute index
	size := sortedSet.Z.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return CreateResult(COk, nil)
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
	slice := sortedSet.Z.RangeByRank(start, stop, desc)
	if withScores {
		result := make([]string, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = element.Member
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = scoreStr
			i++
		}
		res := strings.Join(result, ",")
		return CreateStrResult(COk, res)
	}
	result := make([]string, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = element.Member
		i++
	}
	res := strings.Join(result, ",")
	return CreateStrResult(COk, res)
}

func ZCount(db *SaveDBTables, args []string) Result {
	key := args[0]

	min, err := data.ParseScoreBorder(args[1])
	if err != nil {
		return CreateStrResult(COk, err.Error())
	}

	max, err := data.ParseScoreBorder(args[2])
	if err != nil {
		return CreateStrResult(COk, err.Error())
	}

	// get data
	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	return CreateStrResult(COk, strconv.FormatInt(sortedSet.Z.RangeCount(min, max), 10))
}

func rangeByScore0(db *SaveDBTables, key string, min data.Border, max data.Border, offset int64, limit int64, withScores bool, desc bool) Result {
	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	slice := sortedSet.Z.Range(min, max, offset, limit, desc)
	if withScores {
		result := make([]string, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = element.Member
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = scoreStr
			i++
		}
		res := strings.Join(result, ",")
		return CreateStrResult(COk, res)
	}
	result := make([]string, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = element.Member
		i++
	}
	res := strings.Join(result, ",")
	return CreateStrResult(COk, res)
}

// execZRangeByScore gets members which score within given range, in ascending order
func ZRangeByScore(db *SaveDBTables, args []string) Result {
	if len(args) < 3 {
		return CreateStrResult(CErr, "ERR wrong number of arguments for 'zrangebyscore' command")
	}
	key := args[0]

	min, err := data.ParseScoreBorder(args[1])
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}

	max, err := data.ParseScoreBorder(args[2])
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}

	withScores := false
	var offset int64 = 0
	var limit int64 = -1
	if len(args) > 3 {
		for i := 3; i < len(args); {
			s := args[i]
			if strings.ToUpper(s) == "WITHSCORES" {
				withScores = true
				i++
			} else if strings.ToUpper(s) == "LIMIT" {
				if len(args) < i+3 {
					return CreateStrResult(CErr, "ERR syntax error")
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return CreateStrResult(CErr, "ERR value is not an integer or out of range")
				}
				limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return CreateStrResult(CErr, "ERR value is not an integer or out of range")
				}
				i += 3
			} else {
				return CreateStrResult(CErr, "ERR syntax error")
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, limit, withScores, false)
}

func ZRevRangeByScore(db *SaveDBTables, args []string) Result {
	if len(args) < 3 {
		return CreateStrResult(CErr, "ERR wrong number of arguments for 'zrangebyscore' command")
	}
	key := args[0]

	min, err := data.ParseScoreBorder(args[2])
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}

	max, err := data.ParseScoreBorder(args[1])
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}

	withScores := false
	var offset int64 = 0
	var limit int64 = -1
	if len(args) > 3 {
		for i := 3; i < len(args); {
			s := args[i]
			if strings.ToUpper(s) == "WITHSCORES" {
				withScores = true
				i++
			} else if strings.ToUpper(s) == "LIMIT" {
				if len(args) < i+3 {
					return CreateStrResult(CErr, "ERR syntax error")
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return CreateStrResult(CErr, "ERR value is not an integer or out of range")
				}
				limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return CreateStrResult(CErr, "ERR value is not an integer or out of range")
				}
				i += 3
			} else {
				return CreateStrResult(CErr, "ERR syntax error")
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, limit, withScores, true)
}

func ZRemRangeByScore(db *SaveDBTables, args []string) Result {
	if len(args) != 3 {
		return CreateStrResult(CErr, "ERR wrong number of arguments for 'zremrangebyscore' command")
	}
	key := args[0]

	min, err := data.ParseScoreBorder(args[1])
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}

	max, err := data.ParseScoreBorder(args[2])
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	removed := sortedSet.Z.RemoveRange(min, max)
	if removed > 0 {
		db.addAof(ToCmdLine2("zremrangebyscore", args...))
	}
	return CreateStrResult(COk, strconv.FormatInt(removed, 10))
}

func ZRemRangeByRank(db *SaveDBTables, args []string) Result {
	key := args[0]
	start, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return CreateStrResult(CErr, "ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return CreateStrResult(CErr, "ERR value is not an integer or out of range")
	}

	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	// compute index
	size := sortedSet.Z.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return CreateResult(COk, nil)
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
	removed := sortedSet.Z.RemoveByRank(start, stop)
	if removed > 0 {
		db.addAof(ToCmdLine2("zremrangebyrank", args...))
	}
	return CreateStrResult(COk, strconv.FormatInt(removed, 10))
}

func ZPopMin(db *SaveDBTables, args []string) Result {
	key := string(args[0])
	count := 1
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(args[1])
		if err != nil {
			return CreateStrResult(CErr, "ERR value is not an integer or out of range")
		}
	}

	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	removed := sortedSet.Z.PopMin(count)
	if len(removed) > 0 {
		//persistence
		db.addAof(ToCmdLine2("zpopmin", args...))
	}
	result := make([]string, 0, len(removed)*2)
	for _, element := range removed {
		scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
		result = append(result, element.Member, scoreStr)
	}
	res := strings.Join(result, ",")
	return CreateStrResult(COk, res)
}

// execZRem removes given members
func ZRem(db *SaveDBTables, args []string) Result {
	// parse args
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}

	// get entity
	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	var deleted int64 = 0
	for _, field := range fields {
		if sortedSet.Z.Remove(field) {
			deleted++
		}
	}
	if deleted > 0 {
		//persistence
		db.addAof(ToCmdLine2("zrem", args...))
	}
	return CreateStrResult(COk, strconv.FormatInt(deleted, 10))
}

func ZIncrBy(db *SaveDBTables, args []string) Result {
	key := args[0]
	rawDelta := args[1]
	field := args[2]
	delta, err := strconv.ParseFloat(rawDelta, 64)
	if err != nil {
		return CreateStrResult(CErr, "ERR value is not a valid float")
	}

	// get or init entity
	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	element, exists := sortedSet.Z.Get(field)
	if !exists {
		sortedSet.Z.Add(field, delta)
		//persistence
		db.addAof(ToCmdLine2("zincrby", args...))
		return CreateStrResult(COk, args[1])
	}
	score := element.Score + delta
	sortedSet.Z.Add(field, score)
	//persistence
	db.addAof(ToCmdLine2("zincrby", args...))
	return CreateStrResult(COk, strconv.FormatFloat(score, 'f', -1, 64))
}

func ZLexCount(db *SaveDBTables, args []string) Result {
	key := args[0]
	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	minEle, maxEle := args[1], args[2]
	min, err := data.ParseLexBorder(minEle)
	if err != nil {
		CreateStrResult(CErr, err.Error())
	}
	max, err := data.ParseLexBorder(maxEle)
	if err != nil {
		CreateStrResult(CErr, err.Error())
	}

	count := sortedSet.Z.RangeCount(min, max)

	return CreateStrResult(COk, strconv.FormatInt(count, 10))
}

func ZRangeByLex(db *SaveDBTables, args []string) Result {
	n := len(args)
	if n > 3 && strings.ToLower(args[3]) != "limit" {
		return CreateStrResult(CErr, "ERR syntax error")
	}
	if n != 3 && n != 6 {
		return CreateStrResult(CErr, "ERR wrong number of arguments for 'zrangebylex' command")
	}

	key := args[0]
	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	minEle, maxEle := args[1], args[2]
	min, err := data.ParseLexBorder(minEle)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	max, err := data.ParseLexBorder(maxEle)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}

	offset := int64(0)
	limitCnt := int64(math.MaxInt64)
	if n > 3 {
		var err error
		offset, err = strconv.ParseInt(string(args[4]), 10, 64)
		if err != nil {
			return CreateStrResult(CErr, "ERR value is not an integer or out of range")
		}
		if offset < 0 {
			return CreateResult(COk, nil)
		}
		count, err := strconv.ParseInt(string(args[5]), 10, 64)
		if err != nil {
			return CreateStrResult(CErr, "ERR value is not an integer or out of range")
		}
		if count >= 0 {
			limitCnt = count
		}
	}

	elements := sortedSet.Z.Range(min, max, offset, limitCnt, false)
	result := make([]string, 0, len(elements))
	for _, ele := range elements {
		result = append(result, ele.Member)
	}
	if len(result) == 0 {
		return CreateResult(COk, nil)
	}
	res := strings.Join(result, ",")
	return CreateStrResult(COk, res)
}

func ZRemRangeByLex(db *SaveDBTables, args []string) Result {
	n := len(args)
	if n != 3 {
		return CreateStrResult(CErr, "ERR wrong number of arguments for 'zremrangebylex' command")
	}

	key := args[0]
	sortedSet, err := db.GetZSet(key)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	if sortedSet == nil {
		return CreateStrResult(CErr, "zSet is exists")
	}

	minEle, maxEle := args[1], args[2]
	min, err := data.ParseLexBorder(minEle)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}
	max, err := data.ParseLexBorder(maxEle)
	if err != nil {
		return CreateStrResult(CErr, err.Error())
	}

	count := sortedSet.Z.RemoveRange(min, max)

	return CreateStrResult(COk, strconv.FormatInt(count, 10))
}
