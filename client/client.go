package main

import (
	"bufio"
	"fmt"
	"os"
	"savedb/src"
	"strconv"
	"strings"
	"time"
)

// args[1]=port args[2]=ip
func main() {
	args := os.Args
	ip := "127.0.0.1"
	if len(args) == 3 {
		ip = args[2]
	}
	port, _ := strconv.Atoi(args[1])
	client := src.StartClient(ip, port)
	time.Sleep(time.Second / 10)
	for {
		fmt.Print(ip, ":", port, "->")
		// 创建一个新的读取器，与标准输入绑定
		reader := bufio.NewReader(os.Stdin)
		// 读取用户输入的文本，直到用户按下回车键
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("读取输入时发生错误:", err)
			return
		}
		// 移除输入中的换行符
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}
		// 如果用户输入 'exit'，则退出循环
		if input == "exit" {
			fmt.Println("退出程序。")
			break
		}
		cmd := strings.Split(input, " ")
		if cmd[0] == "expire" {
			expire, err := strconv.ParseInt(cmd[2], 10, 64)
			if err != nil {
				fmt.Println(err)
				return
			}
			ttl := time.Duration(expire*1000) * time.Millisecond
			expireAt := time.Now().Add(ttl)
			cmd[2] = strconv.FormatInt(expireAt.UnixNano()/1e6, 10)
			input = strings.Join(cmd, " ")
		}
		resStr := client.SendMsg(input)
		fmt.Println(resStr)
	}
}
