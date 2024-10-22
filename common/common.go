package common

import (
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"time"
)

var Log *zap.Logger

type Inject struct {
	Values *Values
	Es     *elasticsearch.Client
	Js     nats.JetStreamContext
	Kv     nats.KeyValue
}

type Values struct {
	Elastic `envPrefix:"ELASTIC_"`
	Nats    `envPrefix:"NATS_"`
}

type Elastic struct {
	Hosts    []string `env:"HOSTS,required" envSeparator:","`
	Username string   `env:"USERNAME,required"`
	Password string   `env:"PASSWORD,required"`
}

type Nats struct {
	Hosts []string `env:"HOSTS,required" envSeparator:","`
	Token string   `env:"TOKEN,required"`
}

type Payload struct {
	Timestamp time.Time
	Data      map[string]interface{}
	XData     map[string]interface{}
}
