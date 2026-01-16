package common

import (
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/zap"
)

var Log *zap.Logger

// Inject groups the runtime dependencies required by the collector app.
type Inject struct {
	V        *Values
	Nc       *nats.Conn
	Js       jetstream.JetStream
	Kv       jetstream.KeyValue
	Mc       *mongo.Client
	Db       *mongo.Database
	Schedule gocron.Scheduler
}

// Values is loaded from config/values.yml.
type Values struct {
	Mode        string        `yaml:"mode"`
	Namespace   string        `yaml:"namespace"`
	Description string        `yaml:"description"`
	Duration    time.Duration `yaml:"duration"`
	Batch       int           `yaml:"batch"`
	Nats        Nats          `yaml:"nats"`
	Database    Database      `yaml:"database"`
}

// Nats contains NATS client settings.
type Nats struct {
	Hosts []string `yaml:"hosts"`
	Token string   `yaml:"token"`
}

// Database contains MongoDB client settings.
type Database struct {
	Url  string `yaml:"url"`
	Name string `yaml:"name"`
}
