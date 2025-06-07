package common

import (
	"github.com/go-co-op/gocron/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/zap"
	"time"
)

var Log *zap.Logger

type Inject struct {
	V        *Values
	Nc       *nats.Conn
	Js       jetstream.JetStream
	Kv       jetstream.KeyValue
	Mc       *mongo.Client
	Db       *mongo.Database
	Schedule gocron.Scheduler
}

type Values struct {
	Mode        string        `yaml:"mode"`
	Namespace   string        `yaml:"namespace"`
	Description string        `yaml:"description"`
	Duration    time.Duration `yaml:"duration"`
	Batch       int           `yaml:"batch"`
	Nats        Nats          `yaml:"nats"`
	Database    Database      `yaml:"database"`
}

type Nats struct {
	Hosts []string `yaml:"hosts"`
	Token string   `yaml:"token"`
}

type Database struct {
	Url  string `yaml:"url"`
	Name string `yaml:"name"`
}
