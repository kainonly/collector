package transfer_test

import (
	"context"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/weplanx/collector/v3/transfer"
	"go.mongodb.org/mongo-driver/v2/bson"
	"os"
	"testing"
	"time"
)

var x *transfer.Transfer
var js jetstream.JetStream

func TestMain(m *testing.M) {
	var err error
	if err = MakeTransfer(); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func MakeTransfer() (err error) {
	var nc *nats.Conn
	if nc, err = nats.Connect(
		os.Getenv("NATS_HOSTS"),
		nats.Token(os.Getenv("NATS_TOKEN")),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
	); err != nil {
		return
	}

	if js, err = jetstream.New(nc); err != nil {
		return
	}
	if x, err = transfer.New(context.TODO(), `alpha`, js); err != nil {
		return
	}
	return
}

func TestTransfer_Add(t *testing.T) {
	ctx := context.TODO()
	err := x.Add(ctx, transfer.Option{
		Key:         "audit",
		Description: "审计流",
	})
	assert.Nil(t, err)
}

func TestTransfer_Get(t *testing.T) {
	ctx := context.TODO()
	_, err := x.Get(ctx, `audit`)
	assert.Error(t, err)
	result, err := x.Get(ctx, `audit`)
	assert.Nil(t, err)
	t.Log(result)
}

type MsgData struct {
	ID        bson.ObjectID `bson:"_id"`
	Timestamp time.Time     `bson:"timestamp"`
	Msg       string        `bson:"msg"`
}

func TestTransfer_Send(t *testing.T) {
	err := x.Send(`audit`, MsgData{
		ID:        bson.NewObjectID(),
		Timestamp: time.Now(),
		Msg:       `从生产者到 MongoDB 均不涉及 JSON 转换`,
	})
	assert.NoError(t, err)
}

func TestTransfer_Remove(t *testing.T) {
	ctx := context.TODO()
	err := x.Remove(ctx, "audit")
	assert.Nil(t, err)
}
