package transfer

import (
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"strings"
)

type Transfer struct {
	Namespace string
	Js        nats.JetStreamContext
	Kv        nats.KeyValue
}

func New(namespace string, js nats.JetStreamContext) (x *Transfer, err error) {
	if strings.Contains(namespace, "-") {
		return nil, errors.New(`namespace cannot contain '-'`)
	}
	x = &Transfer{
		Namespace: namespace,
		Js:        js,
	}
	if x.Kv, err = x.Js.KeyValue(x.Namespace); err != nil {
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
	Key         string           `json:"key"`
	Subs        []string         `json:"subs"`
	Description string           `json:"description"`
	Collection  string           `json:"collection"`
	Info        *nats.StreamInfo `json:"info,omitempty"`
}

func (x *Transfer) Get(key string) (option *Option, err error) {
	var entry nats.KeyValueEntry
	if entry, err = x.Kv.Get(key); err != nil {
		return
	}
	if err = sonic.Unmarshal(entry.Value(), &option); err != nil {
		return
	}
	if option.Info, err = x.Js.StreamInfo(x.StreamName(key)); err != nil {
		return
	}
	return
}

func (x *Transfer) Add(option Option) (err error) {
	var b []byte
	if b, err = sonic.Marshal(option); err != nil {
		return
	}
	if _, err = x.Kv.Put(option.Key, b); err != nil {
		return
	}
	subjects := []string{x.SubName(option.Key)}
	for _, sub := range option.Subs {
		subjects = append(subjects, sub)
	}
	if _, err = x.Js.AddStream(&nats.StreamConfig{
		Name:      x.StreamName(option.Key),
		Subjects:  subjects,
		Retention: nats.WorkQueuePolicy,
	}); err != nil {
		return
	}
	return
}

func (x *Transfer) Remove(key string) (err error) {
	if err = x.Kv.Delete(key); err != nil {
		return
	}
	return x.Js.DeleteStream(x.StreamName(key))
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
