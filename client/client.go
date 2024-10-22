package client

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go"
	"github.com/weplanx/collector/common"
)

type Client struct {
	Js nats.JetStreamContext
	Kv nats.KeyValue
}

func New(js nats.JetStreamContext) (x *Client, err error) {
	x = &Client{Js: js}
	if x.Kv, err = x.Js.KeyValue("collector"); err != nil {
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

func (x *Client) Get(key string) (result *Result, err error) {
	result = new(Result)
	var entry nats.KeyValueEntry
	if entry, err = x.Kv.Get(key); err != nil {
		return
	}
	if err = sonic.Unmarshal(entry.Value(), &result.Option); err != nil {
		return
	}
	name := fmt.Sprintf(`COLLECT_%s`, key)
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

	name := fmt.Sprintf(`COLLECT_%s`, option.Key)
	subject := fmt.Sprintf(`collects.%s`, option.Key)

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
	if b, err = sonic.Marshal(option); err != nil {
		return
	}
	if _, err = x.Kv.Put(option.Key, b); err != nil {
		return
	}

	name := fmt.Sprintf(`COLLECT_%s`, option.Key)
	subject := fmt.Sprintf(`collects.%s`, option.Key)

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
	if err = x.Kv.Delete(key); err != nil {
		return
	}
	name := fmt.Sprintf(`COLLECT_%s`, key)
	return x.Js.DeleteStream(name)
}

func (x *Client) Publish(ctx context.Context, key string, payload common.Payload) (err error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err = encoder.Encode(payload); err != nil {
		return
	}
	subject := fmt.Sprintf(`collects.%s`, key)
	if _, err = x.Js.Publish(subject, buf.Bytes(), nats.Context(ctx)); err != nil {
		return
	}
	return
}
