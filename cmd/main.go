package main

import (
	"elastic-collector/app"
	"elastic-collector/app/types"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
)

func main() {
	if _, err := os.Stat("./config/autoload"); os.IsNotExist(err) {
		os.Mkdir("./config/autoload", os.ModeDir)
	}
	if _, err := os.Stat("./config/config.yml"); os.IsNotExist(err) {
		logrus.Fatalln("The service configuration file does not exist")
	}
	cfgByte, err := ioutil.ReadFile("./config/config.yml")
	if err != nil {
		logrus.Fatalln("Failed to read service configuration file", err)
	}
	config := types.Config{}
	err = yaml.Unmarshal(cfgByte, &config)
	if err != nil {
		logrus.Fatalln("Service configuration file parsing failed", err)
	}
	err = app.Application(&config)
	if err != nil {
		logrus.Fatalln("Service failed to start:", err)
	}
}
