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
