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
		Key:         "alpha",
		Description: "开发测试版",
	})
	assert.Nil(t, err)
}

func TestTransfer_Get(t *testing.T) {
	ctx := context.TODO()
	_, err := x.Get(ctx, `beta`)
	assert.Error(t, err)
	result, err := x.Get(ctx, `alpha`)
	assert.Nil(t, err)
	t.Log(result)
}

type OriginData struct {
	ID        bson.ObjectID `bson:"_id"`
	Timestamp time.Time     `bson:"timestamp"`
	Msg       string        `bson:"msg"`
}

//var originData = OriginData{
//	ID:        bson.NewObjectID(),
//	Timestamp: time.Now(),
//	Msg:       `从生产者到 MongoDB 均不涉及 JSON 转换`,
//}

//func TestTransfer_Send(t *testing.T) {
//	var wg sync.WaitGroup
//	wg.Add(1)
//	queueName := x.StreamName(`alpha`)
//	subjectName := x.SubName(`alpha`)
//	go js.QueueSubscribe(subjectName, queueName, func(msg *nats.Msg) {
//		t.Log("get", string(msg.Data))
//		var data OriginData
//		if err := bson.Unmarshal(msg.Data, &data); err != nil {
//			t.Error()
//		}
//		t.Log(data)
//		assert.Equal(t, data.ID.Hex(), originData.ID.Hex())
//		assert.Equal(t, data.Timestamp.UnixNano(), originData.Timestamp.UnixNano())
//		assert.Equal(t, data.Msg, originData.Msg)
//		wg.Done()
//	})
//	time.Sleep(time.Second)
//	err := x.Send("alpha", originData)
//	assert.NoError(t, err)
//	t.Log("send")
//	wg.Wait()
//}

func TestTransfer_Remove(t *testing.T) {
	ctx := context.TODO()
	err := x.Remove(ctx, "beta")
	assert.Nil(t, err)
}
