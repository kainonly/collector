package client_test

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/weplanx/collector/client"
	"github.com/weplanx/collector/common"
	"os"
	"sync"
	"testing"
	"time"
)

import (
	"context"
)

var x *client.Client
var js nats.JetStreamContext

func TestMain(m *testing.M) {
	var err error
	if err = UseNats(context.TODO()); err != nil {
		panic(err)
	}
	if x, err = client.New(js); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func UseNats(ctx context.Context) (err error) {
	var nc *nats.Conn
	if nc, err = nats.Connect(
		os.Getenv("NATS_HOSTS"),
		nats.Token(os.Getenv("NATS_TOKEN")),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
	); err != nil {
		return
	}
	if js, err = nc.JetStream(nats.PublishAsyncMaxPending(256), nats.Context(ctx)); err != nil {
		return
	}
	if os.Getenv("AUTO") == "1" {
		if _, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "collector"}); err != nil {
			return
		}
	} else {
		if _, err = js.KeyValue("collector"); err != nil {
			return
		}
	}
	return
}

func TestTransfer_Set(t *testing.T) {
	err := x.Set(context.TODO(), client.StreamOption{
		Key:         "beta",
		Description: "beta example",
	})
	assert.Nil(t, err)
}

func TestTransfer_Update(t *testing.T) {
	err := x.Update(context.TODO(), client.StreamOption{
		Key:         "beta",
		Description: "beta example 123",
	})
	assert.Nil(t, err)
}

func TestTransfer_Get(t *testing.T) {
	_, err := x.Get("not_exists")
	assert.Error(t, err)
	result, err := x.Get("beta")
	assert.Nil(t, err)
	t.Log(result.Option)
	t.Log(result.Info)
}

func TestTransfer_Publish(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	subjectName := fmt.Sprintf(`collects.%s`, "beta")
	queueName := fmt.Sprintf(`COLLECT_%s`, "beta")
	now := time.Now()
	data := map[string]interface{}{
		"uuid": "0ff5483a-7ddc-44e0-b723-c3417988663f",
		"msg":  "hi",
	}
	go js.QueueSubscribe(subjectName, queueName, func(msg *nats.Msg) {
		t.Log("get", string(msg.Data))
		var payload common.Payload
		if err := gob.NewDecoder(bytes.NewBuffer(msg.Data)).
			Decode(&payload); err != nil {
			t.Error()
		}
		t.Log(payload)
		assert.Equal(t, data, payload.Data)
		assert.Equal(t, now.UnixNano(), payload.Timestamp.UnixNano())
		wg.Done()
	})
	time.Sleep(time.Second)
	err := x.Publish(context.TODO(), "beta", common.Payload{
		Data:      data,
		Timestamp: now,
	})
	assert.NoError(t, err)
	t.Log("send")
	wg.Wait()
}

//func TestTransfer_ManualPublish(t *testing.T) {
//	now := time.Now()
//	err := client.Publish(context.TODO(), "system", transfer.Payload{
//		Timestamp: now,
//		Data: map[string]interface{}{
//			"metadata": map[string]interface{}{
//				"user_id": "640e7c2c7d8a24d6f831e9bf",
//			},
//			"msg": "123456",
//		},
//		Format: map[string]interface{}{
//			"metadata.user_id": "oid",
//		},
//	})
//	assert.NoError(t, err)
//}

//func TestTransfer_ManualPublishNone(t *testing.T) {
//	now := time.Now()
//	err := client.Publish(context.TODO(), "system", transfer.Payload{
//		Timestamp: now,
//		Data: map[string]interface{}{
//			"metadata": map[string]interface{}{
//				"user_id": "",
//			},
//			"msg": "123456",
//		},
//		Format: map[string]interface{}{
//			"metadata.user_id": "oid",
//		},
//	})
//	assert.NoError(t, err)
//}

func TestTransfer_Remove(t *testing.T) {
	err := x.Remove("beta")
	assert.Nil(t, err)
}
