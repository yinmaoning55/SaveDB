package src

import (
	"fmt"
	"testing"
	"time"
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
func TestChannel(t *testing.T) {
	writer := make(chan int, 10)
	go func() {
		writer <- 1
	}()
	go func(w chan int) {
		for {
			select {
			case msg, ok := <-w:
				if !ok {
					fmt.Println("error")
				} else {
					fmt.Println(msg)
				}

			}

		}
	}(writer)
	time.Sleep(time.Second * 10000000)
}
