package mq

import (
	"elastic-collector/app/schema"
	"elastic-collector/app/types"
	"github.com/elastic/go-elasticsearch/v8"
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
	elastic, err := elasticsearch.NewClient(config.Elastic)
	if err != nil {
		return
	}
	dataset := schema.New()
	mqlib, err = NewMessageQueue(config.Mq, elastic, dataset)
	if err != nil {
		return
	}
	os.Exit(m.Run())
}

func TestMessageQueue_Subscribe(t *testing.T) {
	err := mqlib.Subscribe(types.PipeOption{
		Identity: "task-1",
		Index:    "task-1",
		Queue:    "proxy",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMessageQueue_Unsubscribe(t *testing.T) {
	err := mqlib.Unsubscribe("task-1")
	if err != nil {
		t.Fatal(err)
	}
}
