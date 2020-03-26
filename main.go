package main

import (
	"elastic-queue-logger/common"
	"elastic-queue-logger/consumer"
	"elastic-queue-logger/elastic"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
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
	if cfg.Debug {
		go func() {
			http.ListenAndServe(":6060", nil)
		}()
	}
	err = common.SetLogger(&cfg.Log)
	if err != nil {
		log.Fatalln(err)
	}
	bootstrap := consumer.Bootstrap(
		cfg.Amqp,
		elastic.Create(cfg.Elastic),
	)
	defer bootstrap.Close()
	select {}
}
