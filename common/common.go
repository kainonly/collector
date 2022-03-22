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

func SetCLS(option map[string]interface{}) (*cls.AsyncProducerClient, error) {
	producerConfig := cls.GetDefaultAsyncProducerClientConfig()
	producerConfig.Endpoint = option["endpoint"].(string)
	producerConfig.AccessKeyID = option["secretid"].(string)
	producerConfig.AccessKeySecret = option["secretkey"].(string)
	return cls.NewAsyncProducerClient(producerConfig)
}
