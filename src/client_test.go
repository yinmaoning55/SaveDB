package src

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func Test3(t *testing.T) {
	client := StartClient("localhost", 40000)
	time.Sleep(time.Second * 1)
	str := client.SendMsg("fdsfdsfdsfdsgfds")
	fmt.Println("返回:", str)
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
