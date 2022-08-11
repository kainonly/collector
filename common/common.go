package common

import (
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Inject struct {
	Values *Values
	Log    *zap.Logger
	Js     nats.JetStreamContext
	Store  nats.ObjectStore
}

type Values struct {
	// 命名空间
	Namespace string `yaml:"namespace"`

	// NATS 配置
	Nats Nats `yaml:"nats"`

	DataSource DataSource `yaml:"data_source"`
}

type Nats struct {
	Hosts []string `yaml:"hosts"`
	Nkey  string   `yaml:"nkey"`
}

type DataSource struct {
	Type   string                 `yaml:"type"`
	Option map[string]interface{} `yaml:"option"`
}
