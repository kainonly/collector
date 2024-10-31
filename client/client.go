package client

import (
	"context"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"
	"strings"
	"time"
)

type Client struct {
	Namespace string
	Js        nats.JetStreamContext
	Kv        nats.KeyValue
}

type Payload struct {
	Timestamp time.Time              `msgpack:"timestamp"`
	Data      map[string]interface{} `msgpack:"data"`
	XData     map[string]interface{} `msgpack:"xdata"`
}

func New(namespace string, js nats.JetStreamContext) (x *Client, err error) {
	x = &Client{
		Namespace: namespace,
		Js:        js,
	}
	if x.Kv, err = x.Js.KeyValue(x.Namespace); err != nil {
		return
	}
	return
}

type StreamOption struct {
	Key         string `json:"key"`
	Description string `json:"description"`
}

type Result struct {
	Option StreamOption     `json:"option"`
	Info   *nats.StreamInfo `json:"info"`
}

func (x *Client) NamespaceParse() string {
	return strings.Replace(x.Namespace, "-", "_", -1)
}

func (x *Client) Get(key string) (result *Result, err error) {
	result = new(Result)
	var entry nats.KeyValueEntry
	if entry, err = x.Kv.Get(key); err != nil {
		return
	}
	if err = sonic.Unmarshal(entry.Value(), &result.Option); err != nil {
		return
	}
	name := fmt.Sprintf(`%s_%s`, x.NamespaceParse(), key)
	if result.Info, err = x.Js.StreamInfo(name); err != nil {
		return
	}
	return
}

func (x *Client) Set(ctx context.Context, option StreamOption) (err error) {
	var b []byte
	if b, err = sonic.Marshal(option); err != nil {
		return
	}
	if _, err = x.Kv.Put(option.Key, b); err != nil {
		return
	}

	name := fmt.Sprintf(`%s_%s`, x.NamespaceParse(), option.Key)
	subject := fmt.Sprintf(`%s.%s`, x.NamespaceParse(), option.Key)

	if _, err = x.Js.AddStream(&nats.StreamConfig{
		Name:      name,
		Subjects:  []string{subject},
		Retention: nats.WorkQueuePolicy,
	}, nats.Context(ctx)); err != nil {
		return
	}

	return
}

func (x *Client) Update(ctx context.Context, option StreamOption) (err error) {
	var b []byte
	if b, err = sonic.Marshal(option); err != nil {
		return
	}
	if _, err = x.Kv.Put(option.Key, b); err != nil {
		return
	}

	name := fmt.Sprintf(`%s_%s`, x.NamespaceParse(), option.Key)
	subject := fmt.Sprintf(`%s.%s`, x.NamespaceParse(), option.Key)

	if _, err = x.Js.UpdateStream(&nats.StreamConfig{
		Name:      name,
		Subjects:  []string{subject},
		Retention: nats.WorkQueuePolicy,
	}, nats.Context(ctx)); err != nil {
		return
	}

	return
}

func (x *Client) Remove(key string) (err error) {
	if err = x.Kv.Delete(key); err != nil {
		return
	}
	name := fmt.Sprintf(`%s_%s`, x.NamespaceParse(), key)
	return x.Js.DeleteStream(name)
}

func (x *Client) Publish(ctx context.Context, key string, payload Payload) (err error) {
	var b []byte
	if b, err = msgpack.Marshal(payload); err != nil {
		return
	}
	subject := fmt.Sprintf(`%s.%s`, x.NamespaceParse(), key)
	if _, err = x.Js.Publish(subject, b, nats.Context(ctx)); err != nil {
		return
	}
	return
}
