package client

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"
	"time"
)

type Client struct {
	Namespace string
	Js        nats.JetStreamContext
	KeyValue  nats.KeyValue
}

func New(options ...Option) (x *Client, err error) {
	x = new(Client)
	for _, v := range options {
		v(x)
	}
	if x.KeyValue, err = x.Js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: fmt.Sprintf(`%s_logs`, x.Namespace),
	}); err != nil {
		return
	}
	return
}

type Option func(x *Client)

func SetNamespace(v string) Option {
	return func(x *Client) {
		x.Namespace = v
	}
}

func SetJetStream(v nats.JetStreamContext) Option {
	return func(x *Client) {
		x.Js = v
	}
}

type StreamOption struct {
	Key         string `msgpack:"key"`
	Description string `msgpack:"description"`
}

func (x *Client) Get(key string) (result map[string]interface{}, err error) {
	result = make(map[string]interface{})
	var entry nats.KeyValueEntry
	if entry, err = x.KeyValue.Get(key); err != nil {
		return
	}
	var option StreamOption
	if err = msgpack.Unmarshal(entry.Value(), &option); err != nil {
		return
	}
	result["option"] = option
	name := fmt.Sprintf(`%s:logs:%s`, x.Namespace, key)
	var info *nats.StreamInfo
	if info, err = x.Js.StreamInfo(name); err != nil {
		return
	}
	result["info"] = *info
	return
}

func (x *Client) Set(ctx context.Context, option StreamOption) (err error) {
	var b []byte
	if b, err = msgpack.Marshal(option); err != nil {
		return
	}
	if _, err = x.KeyValue.Put(option.Key, b); err != nil {
		return
	}

	name := fmt.Sprintf(`%s:logs:%s`, x.Namespace, option.Key)
	subject := fmt.Sprintf(`%s.logs.%s`, x.Namespace, option.Key)

	if _, err = x.Js.AddStream(&nats.StreamConfig{
		Name:        name,
		Subjects:    []string{subject},
		Description: option.Description,
		Retention:   nats.WorkQueuePolicy,
	}, nats.Context(ctx)); err != nil {
		return
	}

	return
}

func (x *Client) Update(ctx context.Context, option StreamOption) (err error) {
	var b []byte
	if b, err = msgpack.Marshal(option); err != nil {
		return
	}
	if _, err = x.KeyValue.Put(option.Key, b); err != nil {
		return
	}

	name := fmt.Sprintf(`%s:logs:%s`, x.Namespace, option.Key)
	subject := fmt.Sprintf(`%s.logs.%s`, x.Namespace, option.Key)

	if _, err = x.Js.UpdateStream(&nats.StreamConfig{
		Name:        name,
		Subjects:    []string{subject},
		Description: option.Description,
		Retention:   nats.WorkQueuePolicy,
	}, nats.Context(ctx)); err != nil {
		return
	}

	return
}

func (x *Client) Remove(key string) (err error) {
	if err = x.KeyValue.Delete(key); err != nil {
		return
	}
	name := fmt.Sprintf(`%s:logs:%s`, x.Namespace, key)
	return x.Js.DeleteStream(name)
}

type Payload struct {
	Timestamp time.Time              `msgpack:"timestamp"`
	Data      map[string]interface{} `msgpack:"data"`
	XData     map[string]interface{} `msgpack:"xdata"`
}

func (x *Client) Publish(ctx context.Context, key string, payload Payload) (err error) {
	var b []byte
	if b, err = msgpack.Marshal(payload); err != nil {
		return
	}
	subject := fmt.Sprintf(`%s.logs.%s`, x.Namespace, key)
	if _, err = x.Js.Publish(subject, b, nats.Context(ctx)); err != nil {
		return
	}
	return
}
