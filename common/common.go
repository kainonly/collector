package common

import (
	"github.com/nats-io/nats.go"
	cls "github.com/tencentcloud/tencentcloud-cls-sdk-go"
	"go.uber.org/zap"
)

type Inject struct {
	Values *Values
	Log    *zap.Logger
	Nats   *nats.Conn
	Js     nats.JetStreamContext
	CLS    *cls.AsyncProducerClient
}

type Values struct {
	Namespace string `yaml:"namespace"`
	Nats      Nats   `yaml:"nats"`
	CLS       CLS    `yaml:"cls"`
}

type Nats struct {
	Hosts []string `yaml:"hosts"`
	Nkey  string   `yaml:"nkey"`
}

type CLS struct {
	SecretId  string `yaml:"secret_id"`
	SecretKey string `yaml:"secret_key"`
	Endpoint  string `yaml:"endpoint"`
	TopicId   string `yaml:"topic_id"`
}
