package common

import (
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Inject struct {
	Values *Values
	Log    *zap.Logger
	Nats   *nats.Conn
	Js     nats.JetStreamContext
}

type Values struct {
	Namespace string    `yaml:"namespace"`
	Nats      Nats      `yaml:"nats"`
	LogSystem LogSystem `yaml:"log_system"`
}

type Nats struct {
	Hosts []string `yaml:"hosts"`
	Nkey  string   `yaml:"nkey"`
}

type LogSystem struct {
	Type   string                 `yaml:"type"`
	Option map[string]interface{} `yaml:"option"`
}
