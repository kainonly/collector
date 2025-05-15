package common

import (
	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/zap"
)

var Log *zap.Logger

type Inject struct {
	V  *Values
	Mc *mongo.Client
	Db *mongo.Database
	Js nats.JetStreamContext
	Kv nats.KeyValue
}
