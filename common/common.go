package common

import (
	"github.com/elastic/go-elasticsearch/v8"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

var (
	LogOpt *LogOption
)

type (
	AppOption struct {
		Debug   bool                 `yaml:"debug"`
		Amqp    AmqpOption           `yaml:"amqp"`
		Elastic elasticsearch.Config `yaml:"elastic"`
		Log     LogOption            `yaml:"log"`
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
	LogOption struct {
		Storage    bool   `yaml:"storage"`
		StorageDir string `yaml:"storage_dir"`
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

func SetLogger(option *LogOption) (err error) {
	LogOpt = option
	if _, err := os.Stat(option.StorageDir); os.IsNotExist(err) {
		os.Mkdir(option.StorageDir, os.ModeDir)
	}
	return
}

func OpenStorage() bool {
	return LogOpt.Storage
}

func LogFile(identity string) (file *os.File, err error) {
	if _, err := os.Stat("./" + LogOpt.StorageDir + "/" + identity); os.IsNotExist(err) {
		os.Mkdir("./"+LogOpt.StorageDir+"/"+identity, os.ModeDir)
	}
	date := time.Now().Format("2006-01-02")
	filename := "./" + LogOpt.StorageDir + "/" + identity + "/" + date + ".log"
	if _, err = os.Stat(filename); os.IsNotExist(err) {
		file, err = os.Create(filename)
		if err != nil {
			return
		}
	} else {
		file, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err != nil {
			return
		}
	}
	return
}
