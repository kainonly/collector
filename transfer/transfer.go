package transfer

import (
	"context"
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/bson"
	"strings"
)

type Transfer struct {
	Namespace string
	Js        jetstream.JetStream
	Kv        jetstream.KeyValue
}

func New(ctx context.Context, namespace string, js jetstream.JetStream) (x *Transfer, err error) {
	if strings.Contains(namespace, "-") {
		return nil, errors.New(`namespace cannot contain '-'`)
	}
	x = &Transfer{
		Namespace: namespace,
		Js:        js,
	}
	if x.Kv, err = x.Js.KeyValue(ctx, x.Namespace); err != nil {
		return
	}
	return
}

func (x *Transfer) StreamName(key string) string {
	return fmt.Sprintf(`%s_%s`, x.Namespace, key)
}

func (x *Transfer) SubName(key string) string {
	return fmt.Sprintf(`%s.%s`, x.Namespace, key)
}

type Option struct {
	Key         string                `json:"key"`
	Subs        []string              `json:"subs"`
	Description string                `json:"description"`
	Collection  string                `json:"collection"`
	Info        *jetstream.StreamInfo `json:"info,omitempty"`
}

func (x *Transfer) Get(ctx context.Context, key string) (option *Option, err error) {
	var entry jetstream.KeyValueEntry
	if entry, err = x.Kv.Get(ctx, key); err != nil {
		return
	}
	if err = sonic.Unmarshal(entry.Value(), &option); err != nil {
		return
	}
	var stream jetstream.Stream
	if stream, err = x.Js.Stream(ctx, x.StreamName(key)); err != nil {
		return
	}
	if option.Info, err = stream.Info(ctx); err != nil {
		return
	}
	return
}

func (x *Transfer) Add(ctx context.Context, option Option) (err error) {
	subjects := []string{x.SubName(option.Key)}
	for _, sub := range option.Subs {
		subjects = append(subjects, sub)
	}

	var stream jetstream.Stream
	if stream, err = x.Js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        x.StreamName(option.Key),
		Subjects:    subjects,
		Description: option.Description,
		Retention:   jetstream.WorkQueuePolicy,
	}); err != nil {
		return
	}

	if _, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "default",
		AckPolicy: jetstream.AckExplicitPolicy,
	}); err != nil {
		return
	}

	var b []byte
	if b, err = sonic.Marshal(option); err != nil {
		return
	}
	if _, err = x.Kv.Put(ctx, option.Key, b); err != nil {
		return
	}
	return
}

func (x *Transfer) Remove(ctx context.Context, key string) (err error) {
	return x.Kv.Delete(ctx, key)
}

func (x *Transfer) Send(key string, data any) (err error) {
	var content []byte
	if content, err = bson.Marshal(data); err != nil {
		return
	}
	if _, err = x.Js.PublishAsync(x.SubName(key), content); err != nil {
		return
	}
	return
}
