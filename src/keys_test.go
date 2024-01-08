package src

import (
	"fmt"
	"testing"
	"time"
)

func TestKeys(t *testing.T) {
	ttl := time.Duration(60*10*1000) * time.Millisecond
	expireAt := time.Now().Add(ttl)
	fmt.Println(expireAt.Unix())
}

func TestPattern(t *testing.T) {
	// 创建一个map
	myMap := map[string]string{
		"apple":    "red",
		"banana":   "yellow",
		"cherry":   "red",
		"dog":      "brown",
		"elephant": "gray",
	}

	// 定义匹配规则，*表示任意字符串
	pattern := ".*eld.*"

	// 在map中查找满足匹配规则的key
	matchingKeys := findMatchingKeys(myMap, pattern)

	// 打印匹配的key
	fmt.Printf("Keys matching the pattern '%s': %v\n", pattern, matchingKeys)
}
