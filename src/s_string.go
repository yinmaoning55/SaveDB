package src

import (
	"bytes"
	"github.com/tidwall/btree"
	"strings"
)

// 基本的key-value字符串类型使用b树实现
type Str struct {
	btree *btree.BTreeG[*Item]
}
type Item struct {
	key   []byte
	value []byte
}

func NewString() *Str {
	return &Str{
		btree: btree.NewBTreeG[*Item](func(a, b *Item) bool {
			return bytes.Compare(a.key, b.key) == -1
		}),
	}
}

func Get(db *saveDBTables, args []string) Result {
	key := args[0]
	item, ok := db.Str.btree.Get(&Item{key: StringToBytes(key)})
	if ok {
		return CreateResult(C_OK, item.value)
	}
	return CreateStrResult(C_ERR, "key is exist")
}

func SetExc(db *saveDBTables, arg []string) Result {
	db.Str.btree.Set(&Item{key: StringToBytes(arg[0]), value: StringToBytes(arg[1])})
	return CreateResult(C_OK, StringToBytes(OK_STR))
}

func Delete(db *saveDBTables, args []string) Result {
	db.Str.btree.Delete(&Item{key: StringToBytes(args[0])})
	return CreateResult(C_OK, StringToBytes(OK_STR))
}

func All(db *saveDBTables, args []string) Result {
	items := db.Str.btree.Items()
	size := len(items)
	var builder strings.Builder
	for i, item := range items {
		builder.Write(item.key)
		builder.WriteString("=")
		builder.Write(item.value)
		if i+1 < size {
			builder.WriteString(",")
		}
	}
	return CreateResult(C_OK, StringToBytes(builder.String()))
}

//func (bt *Str) PrefixScan(prefix []byte, offset, limitNum int) []*Record {
//	records := make([]*Record, 0)
//
//	bt.btree.Ascend(&Item{key: prefix}, func(item *Item) bool {
//		if !bytes.HasPrefix(item.key, prefix) {
//			return false
//		}
//
//		if offset > 0 {
//			offset--
//			return true
//		}
//
//		records = append(records, item.record)
//
//		limitNum--
//		return limitNum != 0
//	})
//
//	return records
//}
//
//func (bt *Str) PrefixSearchScan(prefix []byte, reg string, offset, limitNum int) []*Record {
//	records := make([]*Record, 0)
//
//	rgx := regexp.MustCompile(reg)
//
//	bt.btree.Ascend(&Item{key: prefix}, func(item *Item) bool {
//		if !bytes.HasPrefix(item.key, prefix) {
//			return false
//		}
//
//		if offset > 0 {
//			offset--
//			return true
//		}
//
//		if !rgx.Match(bytes.TrimPrefix(item.key, prefix)) {
//			return true
//		}
//
//		records = append(records, item.record)
//
//		limitNum--
//		return limitNum != 0
//	})
//
//	return records
//}

func getCommand(m *Message) string {
	return "ok"
}
func setCommand(m *Message) string {
	return "ok"
}
