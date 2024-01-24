package src

import (
	"github.com/robfig/cron/v3"
	"savedb/src/log"
)

type SentinelConfig struct {
	Port int            `yaml:"port"`
	Logs *log.LogConfig `yaml:"logs"`
}

func InitSentinelServer() {
	CronManager = cron.New(cron.WithSeconds())
	CronManager.Start()
}
