package src

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestLFU(t *testing.T) {
	key := "2"
	m := LFUGetTimeInMinutesTest(20)
	o := NewSaveObject2(&key, 1, m, 200)
	LFUDecrAndReturn(o)
}
func NewSaveObject2(key *string, keyType byte, t uint64, times uint32) *SaveObject {
	o := &SaveObject{
		dataType: keyType,
		lru:      uint32(t<<16) | times,
		prt:      key,
	}
	return o
}
func LFUGetTimeInMinutesTest(m int64) uint64 {
	return uint64(((time.Now().UnixMilli() / 1000 / 60) - m) & 65535)
}

var F []string

func TestLFU2(t *testing.T) {
	f := F
	sds(f)
	fmt.Println(F[2])
	f2 := F
	fmt.Println(f2[2])
}
func sds(f []string) {
	f[2] = "dsds"
}
func init() {
	F = make([]string, 10)
	for i := 0; i < 10; i++ {
		F[i] = strconv.Itoa(i)
	}
}
