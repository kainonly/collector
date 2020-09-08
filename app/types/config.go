package types

import "github.com/elastic/go-elasticsearch/v8"

type Config struct {
	Debug   bool                 `yaml:"debug"`
	Listen  string               `yaml:"listen"`
	Elastic elasticsearch.Config `yaml:"elastic"`
	Mq      MqOption             `yaml:"mq"`
}
