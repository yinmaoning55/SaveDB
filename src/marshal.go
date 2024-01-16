package src

import (
	"savedb/src/data"
	"strconv"
	"time"
)

func EntityToCmd(key string, entity any) *MultiBulkReply {
	if entity == nil {
		return nil
	}
	var cmd *MultiBulkReply
	switch entity.(type) {
	case []byte:
		cmd = stringToCmd(key, entity.([]byte))
	case *List:
		cmd = listToCmd(key, entity.(*List))
	case *Set:
		cmd = setToCmd(key, entity.(*Set))
	case *Hash:
		cmd = hashToCmd(key, entity.(*Hash))
	case *ZSet:
		cmd = zSetToCmd(key, entity.(*ZSet))
	}
	return cmd
}

var setCmd = []byte("set")

func stringToCmd(key string, bytes []byte) *MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return MakeMultiBulkReply(args)
}

var rPushAllCmd = []byte("RPUSH")

func listToCmd(key string, list *List) *MultiBulkReply {
	args := make([][]byte, 2+list.L.Len())
	args[0] = rPushAllCmd
	args[1] = []byte(key)
	list.L.ForEach(func(i int, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2+i] = bytes
		return true
	})
	return MakeMultiBulkReply(args)
}

var sAddCmd = []byte("SADD")

func setToCmd(key string, set *Set) *MultiBulkReply {
	args := make([][]byte, 2+len(set.M))
	args[0] = sAddCmd
	args[1] = []byte(key)
	i := 0
	for s, _ := range set.M {
		args[2+i] = []byte(s)
		i++
	}
	return MakeMultiBulkReply(args)
}

var hMSetCmd = []byte("HMSET")

func hashToCmd(key string, hash *Hash) *MultiBulkReply {
	args := make([][]byte, 2+len(hash.M)*2)
	args[0] = hMSetCmd
	args[1] = []byte(key)
	i := 0
	for key, val := range hash.M {
		bytes := []byte(*val)
		args[2+i*2] = []byte(key)
		args[3+i*2] = bytes
		i++
	}
	return MakeMultiBulkReply(args)
}

var zAddCmd = []byte("ZADD")

func zSetToCmd(key string, zset *ZSet) *MultiBulkReply {
	args := make([][]byte, 2+zset.Z.Len()*2)
	args[0] = zAddCmd
	args[1] = []byte(key)
	i := 0
	zset.Z.ForEachByRank(int64(0), int64(zset.Z.Len()), true, func(element *data.Element) bool {
		value := strconv.FormatFloat(element.Score, 'f', -1, 64)
		args[2+i*2] = []byte(value)
		args[3+i*2] = []byte(element.Member)
		i++
		return true
	})
	return MakeMultiBulkReply(args)
}

var pExpireAtBytes = []byte("expire")

// MakeExpireCmd generates command line to set expiration for the given key
func MakeExpireCmd(key string, expireAt time.Time) *MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return MakeMultiBulkReply(args)
}
