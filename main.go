package main

import (
	"elastic-queue-logger/common"
	"elastic-queue-logger/consumer"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"net/http"
	"os"
)

func main() {
	if _, err := os.Stat("./config/autoload"); os.IsNotExist(err) {
		os.Mkdir("./config/autoload", os.ModeDir)
	}
	in, err := ioutil.ReadFile("./config/config.yml")
	if err != nil {
		log.Fatalln(err)
	}
	cfg := common.AppOption{}
	err = yaml.Unmarshal(in, &cfg)
	if err != nil {
		log.Fatalln(err)
	}
	log.Info(cfg.Elastic)
	if cfg.Debug {
		go func() {
			http.ListenAndServe(":6060", nil)
		}()
	}
	bootstrap := consumer.Bootstrap(&cfg.Amqp)
	defer bootstrap.Close()
	select {}
}
