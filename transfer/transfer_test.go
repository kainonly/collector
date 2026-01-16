package transfer_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/kainonly/collector/v3/transfer"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var x *transfer.Transfer

func TestMain(m *testing.M) {
	if os.Getenv("TRANSFER_INTEGRATION") != "1" {
		fmt.Fprintln(os.Stderr, "transfer integration tests are skipped (set TRANSFER_INTEGRATION=1).")
		os.Exit(0)
	}
	if os.Getenv("NATS_HOSTS") == "" {
		fmt.Fprintln(os.Stderr, "transfer integration tests are skipped (missing NATS_HOSTS).")
		os.Exit(0)
	}

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
		nats.Timeout(3*time.Second),
	); err != nil {
		return
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	if _, err = js.CreateOrUpdateKeyValue(context.TODO(), jetstream.KeyValueConfig{
		Bucket:      "alpha",
		Description: "test bucket",
		History:     3,
		Compression: true,
	}); err != nil {
		return err
	}

	if x, err = transfer.New(context.TODO(), `alpha`, nc); err != nil {
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
	result, err := x.Get(ctx, `audit`)
	assert.NoError(t, err)
	t.Log(result)
	t.Log(result.Nexts)
	t.Log(result.Last)
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
