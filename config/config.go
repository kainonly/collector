package config

import (
	"elastic-collector/application/service/queue"
	"github.com/elastic/go-elasticsearch/v8"
)

type Config struct {
	Debug   string               `yaml:"debug"`
	Listen  string               `yaml:"listen"`
	Gateway string               `yaml:"gateway"`
	Elastic elasticsearch.Config `yaml:"elastic"`
	Queue   queue.Option         `yaml:"queue"`
}
