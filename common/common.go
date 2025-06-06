package common

import (
	"github.com/go-co-op/gocron/v2"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/zap"
)

var Log *zap.Logger

type Inject struct {
	V        *Values
	Js       jetstream.JetStream
	Kv       jetstream.KeyValue
	Mc       *mongo.Client
	Db       *mongo.Database
	Schedule gocron.Scheduler
}
