package src

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"
	"unsafe"
)

func Test1(t *testing.T) {
	buff := make([]int, 10)
	buff[0] = 0
	buff[1] = 1
	buff[2] = 2
	buff[3] = 3
	buff[4] = 4
	buff[5] = 5
	fmt.Println(buff[2:6])
}
func Test2(t *testing.T) {

}

type TestS struct {
	name *string
	num  int
	maps map[string]string
}

func Test2Caches(t *testing.T) {
	instan := &TestS{maps: make(map[string]string)}
	go func(t *TestS) {
		for i := 0; i < 10000000; i++ {
			time.Sleep(time.Second)
			t.num = i
			str := fmt.Sprintf("%d %s", i, "name")
			t.name = &str
			t.maps[str] = str
		}

	}(instan)
	go func(t *TestS) {
		for i := 0; i < 10000000; i++ {
			time.Sleep(time.Second)
			fmt.Println(t.num)
			if t.name == nil {
				continue
			}
			fmt.Println(*t.name)
			fmt.Println("map大小:", len(t.maps))
		}
	}(instan)
	time.Sleep(time.Second * 1000000)
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

type S struct {
	A uint32
	B uint64
	C uint64
	D uint64
	E struct{}
}

func TestByte(t *testing.T) {
	fmt.Println(unsafe.Offsetof(S{}.E))
	fmt.Println(unsafe.Sizeof(S{}.E))
	fmt.Println(unsafe.Sizeof(S{}))
}
func TestByte2(t *testing.T) {
	str := "1"
	data := make([]byte, len([]byte(str))+2+4)
	binary.BigEndian.PutUint16(data[:2], C_OK)
	binary.BigEndian.PutUint32(data[2:6], uint32(len([]byte(str))))
	copy(data[6:], str)
	fmt.Println(data)
}
