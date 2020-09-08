package mq

import (
	"elastic-transfer/app/types"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

var mqlib *MessageQueue

func TestMain(m *testing.M) {
	os.Chdir("../..")
	var err error
	if _, err := os.Stat("./config/autoload"); os.IsNotExist(err) {
		os.Mkdir("./config/autoload", os.ModeDir)
	}
	cfgByte, err := ioutil.ReadFile("./config/config.yml")
	if err != nil {
		log.Fatalln("Failed to read service configuration file", err)
	}
	config := types.Config{}
	err = yaml.Unmarshal(cfgByte, &config)
	if err != nil {
		log.Fatalln("Service configuration file parsing failed", err)
	}
	mqlib, err = NewMessageQueue(config.Mq)
	if err != nil {
		return
	}
	os.Exit(m.Run())
}

func TestMessageQueue_Push(t *testing.T) {
	err := mqlib.Push("test", "", []byte(`{"name":"kain"}`))
	if err != nil {
		log.Fatalln(err)
	}
}
