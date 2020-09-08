package manage

import (
	"elastic-collector/app/mq"
	"elastic-collector/app/schema"
	"elastic-collector/app/types"
	"github.com/elastic/go-elasticsearch/v8"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

var manager *ElasticManager

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
	mqlib, err := mq.NewMessageQueue(config.Mq, elastic, dataset)
	if err != nil {
		return
	}
	manager, err = NewElasticManager(elastic, mqlib, dataset)
	if err != nil {
		log.Fatalln(err)
	}
	os.Exit(m.Run())
}

func TestElasticManager_Put(t *testing.T) {
	err := manager.Put(types.PipeOption{
		Identity: "task",
		Index:    "task-log",
		Queue:    `schedule`,
	})
	if err != nil {
		t.Error(err)
	}
}

func TestElasticManager_Delete(t *testing.T) {
	err := manager.Delete("task")
	if err != nil {
		t.Error(err)
	}
}
