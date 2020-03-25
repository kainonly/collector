package common

import (
	"github.com/elastic/go-elasticsearch/v8"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"path/filepath"
)

type (
	AppOption struct {
		Debug   bool                 `yaml:"debug"`
		Amqp    AmqpOption           `yaml:"amqp"`
		Elastic elasticsearch.Config `yaml:"elastic"`
	}
	AmqpOption struct {
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		Vhost    string `yaml:"vhost"`
	}
	ConsumerOption struct {
		Identity string `yaml:"identity"`
		Queue    string `yaml:"queue"`
		Index    string `yaml:"index"`
	}
)

func ListConsumerOption() (list []ConsumerOption, err error) {
	var files []os.FileInfo
	files, err = ioutil.ReadDir("./config/autoload")
	if err != nil {
		return
	}
	for _, file := range files {
		ext := filepath.Ext(file.Name())
		if ext == ".yml" {
			var in []byte
			in, err = ioutil.ReadFile("./config/autoload/" + file.Name())
			if err != nil {
				return
			}
			var config ConsumerOption
			err = yaml.Unmarshal(in, &config)
			if err != nil {
				return
			}
			list = append(list, config)
		}
	}
	return
}
