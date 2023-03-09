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
	Database  string `env:"DATABASE,required"`
	Nats      `envPrefix:"NATS_"`
}

type Nats struct {
	Hosts []string `env:"HOSTS,required" envSeparator:","`
	Nkey  string   `env:"NKEY,required"`
}
