package common

import (
	"encoding/json"
	"log"
	"os"
)

var ServerConfig *serverConfig = &serverConfig{}

type serverConfig struct {
	ServerIp string     `json:"serverIp"`
	Port     int        `json:"port"`
	Logs     *LogConfig `json:"logs"`
}

func (config *serverConfig) LoadConfig(path string) {
	cfile, cferr := os.Open(path)
	if cferr != nil {
		log.Fatal("Open config file error", cferr.Error())
		return
	}

	defer cfile.Close()
	decoder := json.NewDecoder(cfile)
	if err := decoder.Decode(config); err != nil {
		log.Fatal("Decode GSConfig file error", err.Error())
	}
	log.Println("init config!", config)
}
