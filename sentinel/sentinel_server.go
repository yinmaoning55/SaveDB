package main

import (
	"os"
	"os/signal"
	"savedb/src"
	"savedb/src/log"
	"syscall"
	"time"
)

func main() {
	args := os.Args
	path := "config/sentinel.yaml"
	if len(args) == 2 {
		path = args[1]
	}

	src.SConfig.LoadSentinelConfig(path)

	log.InitLog(src.SConfig.Logs)
	log.SaveDBLogger.Infof("sentinel init config!", src.Config)

	src.InitSentinelServer()
	log.SaveDBLogger.Infof("init sentinel server!", src.Config)

	err := src.StartTCPServer(src.Config.Port)
	if err != nil {
		log.SaveDBLogger.Error("sentinel server start fail err=", err)
		return
	}

	banner := "   ___________ _   _________    _____________   ___  ________  _____    ______  ________________________\n  / __/ __/ _ \\ | / / __/ _ \\  / __/_  __/ _ | / _ \\/_  __/ / / / _ \\  / __/ / / / ___/ ___/ __/ __/ __/\n _\\ \\/ _// , _/ |/ / _// , _/ _\\ \\  / / / __ |/ , _/ / / / /_/ / ___/ _\\ \\/ /_/ / /__/ /__/ _/_\\ \\_\\ \\  \n/___/___/_/|_||___/___/_/|_| /___/ /_/ /_/ |_/_/|_| /_/  \\____/_/    /___/\\____/\\___/\\___/___/___/___/  "
	println(banner)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		select {
		case s := <-sig:
			log.SaveDBLogger.Infof("sentinel server will stop...")
			//停止所有的读写
			src.TcpServer.Close.Store(true)
			time.Sleep(time.Second)
			//持久化
			log.SaveDBLogger.Infof("sentinel server stopped by", s.String())
			return
		}
	}
}
