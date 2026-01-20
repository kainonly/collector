package bootstrap

import (
	"context"
	"os"
	"strings"

	"github.com/kainonly/collector/v3/common"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

func SetZap() (log *zap.Logger, err error) {
	if os.Getenv("MODE") != "release" {
		if log, err = zap.NewDevelopment(); err != nil {
			return
		}
	} else {
		if log, err = zap.NewProduction(); err != nil {
			return
		}
	}
	return
}

// LoadStaticValues 从 ./config/values.yml 加载配置
func LoadStaticValues() (v *common.Values, err error) {
	v = new(common.Values)
	var b []byte
	if b, err = os.ReadFile("./config/values.yml"); err != nil {
		return
	}
	if err = yaml.Unmarshal(b, &v); err != nil {
		return
	}
	return
}

// UseMongo 创建 MongoDB 客户端
func UseMongo(v *common.Values) (*mongo.Client, error) {
	return mongo.Connect(
		options.Client().ApplyURI(v.MongoUrl),
	)
}

// UseNats 创建 NATS 连接，支持无限重连
func UseNats(values *common.Values) (nc *nats.Conn, err error) {
	if nc, err = nats.Connect(
		strings.Join(values.NatsHosts, ","),
		nats.Token(values.NatsToken),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
	); err != nil {
		return
	}
	return
}

// UseJetStream 创建 JetStream 上下文
func UseJetStream(nc *nats.Conn) (jetstream.JetStream, error) {
	return jetstream.New(nc)
}

// UseKeyValue 创建或更新用于流配置的命名空间 KV 存储桶
func UseKeyValue(values *common.Values, js jetstream.JetStream) (jetstream.KeyValue, error) {
	return js.CreateOrUpdateKeyValue(context.TODO(), jetstream.KeyValueConfig{
		Bucket:      values.Namespace,
		History:     3,
		Compression: true,
	})
}

// UseDatabase 返回带有 majority 写入关注的数据库句柄
func UseDatabase(v *common.Values, client *mongo.Client) *mongo.Database {
	option := options.Database().
		SetWriteConcern(writeconcern.Majority())
	return client.Database(v.MongoDatabase, option)
}
