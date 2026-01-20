package common

import (
	"time"

	"go.uber.org/zap"
)

var Log *zap.Logger

// Values 从 config/values.yml 加载
type Values struct {
	Mode          string        `yaml:"mode"`
	Namespace     string        `yaml:"namespace"`
	BatchSize     int           `yaml:"batch_size"`
	FlushInterval time.Duration `yaml:"flush_interval"`
	NatsHosts     []string      `yaml:"nats_hosts"`
	NatsToken     string        `yaml:"nats_token"`
	MongoUrl      string        `yaml:"mongo_url"`
	MongoDatabase string        `yaml:"mongo_database"`
}
