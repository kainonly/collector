package common

import (
	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

type Inject struct {
	Values    *Values
	Log       *zap.Logger
	Db        *mongo.Database
	JetStream nats.JetStreamContext
	KeyValue  nats.KeyValue
}

type Values struct {
	Namespace string `env:"NAMESPACE,required"`
	Database  struct {
		Url  string `env:"URL,required"`
		Name string `env:"NAME,required"`
	} `envPrefix:"DATABASE_"`
	Nats struct {
		Hosts []string `env:"HOSTS,required" envSeparator:","`
		Nkey  string   `env:"NKEY,required"`
	} `envPrefix:"NATS_"`
}
