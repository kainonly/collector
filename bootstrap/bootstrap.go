package bootstrap

import (
	"context"
	"fmt"
	"github.com/caarlos0/env/v6"
	"github.com/google/wire"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/weplanx/collector/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.uber.org/zap"
	"os"
	"strings"
	"time"
)

var Provides = wire.NewSet(
	LoadStaticValues,
	UseZap,
	UseMongoDB,
	UseDatabase,
	UseNats,
	UseJetStream,
	UseKeyValue,
)

func LoadStaticValues() (values *common.Values, err error) {
	values = new(common.Values)
	if err = env.Parse(values); err != nil {
		return
	}
	return
}

// https://pkg.go.dev/go.uber.org/zap
func UseZap() (log *zap.Logger, err error) {
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

// https://www.mongodb.com/docs/drivers/go/current/
// https://pkg.go.dev/go.mongodb.org/mongo-driver/mongo
func UseMongoDB(values *common.Values) (*mongo.Client, error) {
	return mongo.Connect(
		context.TODO(),
		options.Client().ApplyURI(values.Database),
	)
}

// https://www.mongodb.com/docs/drivers/go/current/
// https://pkg.go.dev/go.mongodb.org/mongo-driver/mongo
func UseDatabase(values *common.Values, client *mongo.Client) (db *mongo.Database) {
	option := options.Database().
		SetWriteConcern(writeconcern.Majority())
	return client.Database(values.Namespace, option)
}

// https://docs.nats.io/using-nats/developer
// SDK https://github.com/nats-io/nats.go
func UseNats(values *common.Values) (nc *nats.Conn, err error) {
	var kp nkeys.KeyPair
	if kp, err = nkeys.FromSeed([]byte(values.Nats.Nkey)); err != nil {
		return
	}
	defer kp.Wipe()
	var pub string
	if pub, err = kp.PublicKey(); err != nil {
		return
	}
	if !nkeys.IsValidPublicUserKey(pub) {
		return nil, fmt.Errorf("nkey verification failed")
	}
	if nc, err = nats.Connect(
		strings.Join(values.Nats.Hosts, ","),
		nats.MaxReconnects(5),
		nats.ReconnectWait(2*time.Second),
		nats.ReconnectJitter(500*time.Millisecond, 2*time.Second),
		nats.Nkey(pub, func(nonce []byte) ([]byte, error) {
			sig, _ := kp.Sign(nonce)
			return sig, nil
		}),
	); err != nil {
		return
	}
	return
}

// https://docs.nats.io/using-nats/developer/develop_jetstream
func UseJetStream(nc *nats.Conn) (nats.JetStreamContext, error) {
	return nc.JetStream(nats.PublishAsyncMaxPending(256))
}

// https://docs.nats.io/using-nats/developer/develop_jetstream/kv
func UseKeyValue(values *common.Values, js nats.JetStreamContext) (nats.KeyValue, error) {
	return js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: fmt.Sprintf(`%s_logs`, values.Namespace),
	})
}
