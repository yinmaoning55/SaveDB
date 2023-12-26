package main

import (
	"os"
	"os/signal"
	"savedb/common"
	logs "savedb/common"
	"syscall"
	"time"
)

func main() {
	common.ServerConfig.LoadConfig("config/config.json")
	common.InitLog(common.ServerConfig.Logs)
	logs.NetLogger.Infof("server start.")
	banner := "   ___________ _   _________    _____________   ___  ________  _____    ______  ________________________\n  / __/ __/ _ \\ | / / __/ _ \\  / __/_  __/ _ | / _ \\/_  __/ / / / _ \\  / __/ / / / ___/ ___/ __/ __/ __/\n _\\ \\/ _// , _/ |/ / _// , _/ _\\ \\  / / / __ |/ , _/ / / / /_/ / ___/ _\\ \\/ /_/ / /__/ /__/ _/_\\ \\_\\ \\  \n/___/___/_/|_||___/___/_/|_| /___/ /_/ /_/ |_/_/|_| /_/  \\____/_/    /___/\\____/\\___/\\___/___/___/___/  "
	println(banner)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		select {
		case s := <-sig:
			common.NetLogger.Infof("server will stop...")
			time.Sleep(time.Second)
			//持久化
			common.NetLogger.Infof("server stopped by", s.String())
			return
		}
	}
}
