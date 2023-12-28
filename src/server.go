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

// 所有的命令 基本上和redis一样
type saveCommands struct {
}

func main() {
	Config.loadConfig("config/config.yaml")
	InitLog(Config.Logs)
	SaveDBLogger.Infof("init config!", Config)
	err := StartTCPServer(Config.Port)
	if err != nil {
		SaveDBLogger.Error("tcp server start fail err=", err)
		return
	}
	banner := "   ___________ _   _________    _____________   ___  ________  _____    ______  ________________________\n  / __/ __/ _ \\ | / / __/ _ \\  / __/_  __/ _ | / _ \\/_  __/ / / / _ \\  / __/ / / / ___/ ___/ __/ __/ __/\n _\\ \\/ _// , _/ |/ / _// , _/ _\\ \\  / / / __ |/ , _/ / / / /_/ / ___/ _\\ \\/ /_/ / /__/ /__/ _/_\\ \\_\\ \\  \n/___/___/_/|_||___/___/_/|_| /___/ /_/ /_/ |_/_/|_| /_/  \\____/_/    /___/\\____/\\___/\\___/___/___/___/  "
	println(banner)

	//在主线程中处理所有的读写操作

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

type saveServer struct {
}
