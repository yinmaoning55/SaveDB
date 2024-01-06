package src

import (
	"fmt"
	"testing"
	"time"
)

func TestRand(t *testing.T) {
	for {
		fmt.Println(zslRandomLevel())
		time.Sleep(time.Second / 10)
	}

}
