package transfer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Transfer 是生产者端的客户端 SDK，用于管理流和发布消息
type Transfer struct {
	Namespace string
	Nc        *nats.Conn
	Js        jetstream.JetStream
	Kv        jetstream.KeyValue
}

// New 创建绑定到命名空间 KV 存储桶的 Transfer 实例
func New(ctx context.Context, namespace string, nc *nats.Conn, opts ...jetstream.JetStreamOpt) (x *Transfer, err error) {
	if strings.Contains(namespace, "-") {
		return nil, errors.New(`namespace 不能包含 '-'`)
	}
	x = &Transfer{Namespace: namespace, Nc: nc}
	if x.Js, err = jetstream.New(nc, opts...); err != nil {
		return
	}
	if x.Kv, err = x.Js.KeyValue(ctx, x.Namespace); err != nil {
		return
	}
	return
}

// StreamName 返回流名称
func (x *Transfer) StreamName(key string) string {
	return fmt.Sprintf(`%s_%s`, x.Namespace, key)
}

// SubName 返回订阅主题名称
func (x *Transfer) SubName(key string) string {
	return fmt.Sprintf(`%s.%s`, x.Namespace, key)
}

// Option 是流配置选项
type Option struct {
	Key         string   `json:"key"`
	Subs        []string `json:"subs"`
	Description string   `json:"description"`
	Collection  string   `json:"collection"`
	*State
}

// State 是收集器状态
type State struct {
	BufferSize int `json:"buffer_size"`
}

// Get 从 KV 获取配置，并通过收集器查询状态数据
func (x *Transfer) Get(ctx context.Context, key string) (option *Option, err error) {
	var entry jetstream.KeyValueEntry
	if entry, err = x.Kv.Get(ctx, key); err != nil {
		return
	}
	if err = sonic.Unmarshal(entry.Value(), &option); err != nil {
		return
	}
	var msg *nats.Msg
	if msg, err = x.Nc.Request(fmt.Sprintf(`%s.states`, x.Namespace), []byte(key), 15*time.Second); err != nil {
		return
	}
	if err = sonic.Unmarshal(msg.Data, &option.State); err != nil {
		return
	}
	return
}

// Add 创建/更新工作队列流并将配置持久化到 KV
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

// Send 发布 BSON 编码的数据到指定 key 的主题
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

// Remove 从 KV 删除配置（收集器会通过 KV 监听自动取消订阅）
func (x *Transfer) Remove(ctx context.Context, key string) (err error) {
	return x.Kv.Delete(ctx, key)
}
