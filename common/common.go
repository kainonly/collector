package common

import (
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Inject struct {
	Values *Values
	Log    *zap.Logger
	Js     nats.JetStreamContext
	Store  nats.ObjectStore
	Influx influxdb2.Client
}

type Values struct {
	// 命名空间
	Namespace string `yaml:"namespace"`

	// NATS 配置
	Nats Nats `yaml:"nats"`

	// Influx 配置
	Influx Influx `yaml:"influx"`
}

type Nats struct {
	Hosts []string `yaml:"hosts"`
	Nkey  string   `yaml:"nkey"`
}

type Influx struct {
	Url    string `yaml:"url"`
	Token  string `yaml:"token"`
	Org    string `yaml:"org"`
	Bucket string `yaml:"bucket"`
}
