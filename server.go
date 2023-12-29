package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var commandTables = []string{"get", "set"}
var saveCommandMap map[string]saveDBCommand

// 所有的命令 基本上和redis一样
type saveDBCommand struct {
	name            string                  //参数名字
	saveCommandProc func(m *Message) string //执行的函数
	arity           int                     //参数个数
}

func main() {
	args := os.Args
	path := "config/config.yaml"
	if len(args) == 2 {
		path = args[1]
	}

	Config.loadConfig(path)

	InitLog(Config.Logs)
	SaveDBLogger.Infof("init config!", Config)

	initCommand()

	err := StartTCPServer(Config.Port)
	if err != nil {
		SaveDBLogger.Error("tcp server start fail err=", err)
		return
	}
	banner := "   ___________ _   _________    _____________   ___  ________  _____    ______  ________________________\n  / __/ __/ _ \\ | / / __/ _ \\  / __/_  __/ _ | / _ \\/_  __/ / / / _ \\  / __/ / / / ___/ ___/ __/ __/ __/\n _\\ \\/ _// , _/ |/ / _// , _/ _\\ \\  / / / __ |/ , _/ / / / /_/ / ___/ _\\ \\/ /_/ / /__/ /__/ _/_\\ \\_\\ \\  \n/___/___/_/|_||___/___/_/|_| /___/ /_/ /_/ |_/_/|_| /_/  \\____/_/    /___/\\____/\\___/\\___/___/___/___/  "
	println(banner)
	//一个协程负责所有的读写逻辑
	go mainGoroutine()
	SaveDBLogger.Infof("server start successful!")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		select {
		case s := <-sig:
			SaveDBLogger.Infof("server will stop...")
			//停止所有的读写
			tcpServer.Close.Store(true)
			time.Sleep(time.Second)
			//持久化
			SaveDBLogger.Infof("server stopped by", s.String())
			return
		}
	}
}

var Config *serverConfig = &serverConfig{}

type serverConfig struct {
	Port int        `json:"port"`
	Logs *LogConfig `json:"logs"`
}

func (config *serverConfig) loadConfig(path string) {
	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Println("Open config file error", err.Error())
		return
	}
	e := yaml.Unmarshal(yamlFile, config)
	if e != nil {
		fmt.Println("read config file erro", err.Error())
		return
	}
	config.Logs.DefaultLevel = "info"
}

var server = &saveServer{}

type saveServer struct {
	Read chan *Message
}

func mainGoroutine() {
	select {
	case msg, ok := <-server.Read:
		conn := tcpServer.Connections[*msg.Conn]
		if !ok {
			if conn.Close.Load() {
				conn.ConnClose()
			}
		} else {
			//逻辑处理

			//写回
			wMsg := &Message{}
			tcpServer.Connections[*msg.Conn].Writer <- wMsg
		}
	}
}
func initCommand() {
	saveCommandMap = make(map[string]saveDBCommand)
	saveCommandMap[commandTables[0]] = saveDBCommand{name: commandTables[0], saveCommandProc: getCommand, arity: 1}
	saveCommandMap[commandTables[1]] = saveDBCommand{name: commandTables[1], saveCommandProc: setCommand, arity: 1}
}
