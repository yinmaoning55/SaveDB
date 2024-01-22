package src

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func Test3(t *testing.T) {
	expire, err := strconv.ParseInt("1705477164630", 10, 64)
	fmt.Println(expire)
	if err != nil {
		fmt.Println(err)
		return
	}
	ttl := time.Duration(expire*1000) * time.Millisecond
	expireAt := time.Now().Add(ttl)
	s := strconv.FormatInt(expireAt.UnixNano()/1e6, 10)
	fmt.Println(s)
}
func TestClinet(t *testing.T) {
	args := os.Args
	ip := "127.0.0.1"
	if len(args) == 3 {
		ip = args[2]
	}
	fmt.Println("-------", args)
	//port, _ := strconv.Atoi(args[1])
	client := StartClient(ip, 40000)
	for {
		// 创建一个新的读取器，与标准输入绑定
		fmt.Print("redis-cli> ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		input := scanner.Text()
		// 移除输入中的换行符
		input = strings.TrimSpace(input)
		// 如果用户输入 'exit'，则退出循环
		if input == "exit" {
			fmt.Println("退出程序。")
			break
		}
		resStr := client.SendMsg(input)
		fmt.Println(resStr)
	}

}

//func TestClient2(t *testing.T) {
//	// 获取标准输入的文件描述符
//	fd := int(os.Stdin.Fd())
//
//	// 设置终端模式
//	oldState, err := term.MakeRaw(fd)
//	if err != nil {
//		fmt.Println("Error setting raw mode:", err)
//		return
//	}
//	defer term.Restore(fd, oldState)
//
//	fmt.Print("Press 'q' to quit\n")
//	for {
//		// 读取单个字符
//		char, key, err := term.ReadKey(fd)
//		if err != nil {
//			fmt.Println("Error reading key:", err)
//			return
//		}
//
//		switch {
//		case key == term.KeyArrowLeft:
//			fmt.Println("Left arrow pressed")
//		case key == term.KeyArrowRight:
//			fmt.Println("Right arrow pressed")
//		case char == 'q':
//			fmt.Println("Quitting...")
//			return
//		default:
//			fmt.Printf("Unknown key: %c\n", char)
//		}
//	}
//}
