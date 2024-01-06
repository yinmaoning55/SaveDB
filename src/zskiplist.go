package src

import (
	"math"
	"math/rand"
)

type ZSkipList struct {
}

// 随机跳表的层数
func zslRandomLevel() int {
	level := 1
	for rand.Intn(0xFFFF) < int(math.Floor(ZSKIPLIST_P*0xFFFF)) {
		level++
	}

	if level < ZSKIPLIST_MAXLEVEL {
		return level
	}
	return ZSKIPLIST_MAXLEVEL
}
