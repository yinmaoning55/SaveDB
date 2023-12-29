package main

import (
	"fmt"
	"testing"
)

func Test1(t *testing.T) {
	buff := make([]int, 10)
	buff[0] = 0
	buff[1] = 1
	buff[2] = 2
	buff[3] = 3
	buff[4] = 4
	buff[5] = 5
	fmt.Println(buff)
	fmt.Println(buff[1:])
	b := []byte("dsds")
	fmt.Println(b)
}
