package src

import (
	"bytes"
	"github.com/tidwall/btree"
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

func (bt *Str) Get(key string) (string, bool) {
	item, ok := bt.btree.Get(&Item{key: StringToBytes(key)})
	if ok {
		return BytesToString(item.value), ok
	}
	return "", ok
}

func (bt *Str) Set(key, value string) bool {
	_, replaced := bt.btree.Set(&Item{key: StringToBytes(key), value: StringToBytes(value)})
	return replaced
}

func (bt *Str) Delete(key string) bool {
	_, deleted := bt.btree.Delete(&Item{key: StringToBytes(key)})
	return deleted
}

func (bt *Str) All() *map[string]string {
	items := bt.btree.Items()

	values := make(map[string]string, len(items))
	for _, item := range items {
		values[BytesToString(item.key)] = BytesToString(item.value)
	}

	return &values
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
