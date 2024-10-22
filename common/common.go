package common

import (
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

var Log *zap.Logger

type Inject struct {
	V  *Values
	Es *elasticsearch.Client
	Js nats.JetStreamContext
	Kv nats.KeyValue
}
