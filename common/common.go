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
	// 命名空间
	Namespace string `env:"NAMESPACE,required"`

	// MongoDB 连接 Uri
	Database string `env:"DATABASE,required"`

	// NATS 配置
	Nats `envPrefix:"NATS_"`
}

type Nats struct {
	// Nats 连接地址
	Hosts []string `env:"HOSTS,required" envSeparator:","`

	// Nats Nkey 认证
	Nkey string `env:"NKEY,required"`
}
