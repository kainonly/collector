// Package transfer 提供生产者端的客户端 SDK。
//
// 用于管理 JetStream 流和向收集器发布 BSON 数据。
//
// 基本用法：
//
//	// 创建客户端
//	t, _ := transfer.New(ctx, "alpha", nc)
//
//	// 注册流
//	t.Add(ctx, transfer.Option{
//	    Key:        "metrics",
//	    Collection: "metrics",
//	})
//
//	// 发布数据
//	t.Send("metrics", bson.M{"cpu": 0.42})
//
//	// 移除流
//	t.Remove(ctx, "metrics")
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

// Transfer 是生产者端的客户端 SDK。
//
// 提供流管理和消息发布功能：
//   - Add: 创建/更新流和消费者
//   - Send: 发布 BSON 数据到流
//   - Get: 查询流配置和收集器状态
//   - Remove: 删除流配置
type Transfer struct {
	// Namespace 应用命名空间，用于流和主题命名
	Namespace string
	// Nc NATS 连接
	Nc *nats.Conn
	// Js JetStream 上下文
	Js jetstream.JetStream
	// Kv 命名空间 KV 存储桶
	Kv jetstream.KeyValue
}

// New 创建绑定到命名空间 KV 存储桶的 Transfer 实例。
//
// 参数：
//   - ctx: 上下文
//   - namespace: 命名空间（不能包含连字符 '-'）
//   - nc: NATS 连接
//   - opts: JetStream 选项（可选）
//
// 注意：命名空间必须与收集器配置的命名空间一致。
// KV 存储桶必须已存在（由收集器或手动创建）。
func New(ctx context.Context, namespace string, nc *nats.Conn, opts ...jetstream.JetStreamOpt) (x *Transfer, err error) {
	// 验证命名空间格式
	// 连字符在 NATS 主题中有特殊含义，禁止使用
	if strings.Contains(namespace, "-") {
		return nil, errors.New(`namespace 不能包含 '-'`)
	}
	x = &Transfer{Namespace: namespace, Nc: nc}
	if x.Js, err = jetstream.New(nc, opts...); err != nil {
		return
	}
	// 获取已存在的 KV 存储桶
	if x.Kv, err = x.Js.KeyValue(ctx, x.Namespace); err != nil {
		return
	}
	return
}

// StreamName 根据 key 生成 JetStream 流名称。
//
// 格式：{namespace}_{key}
// 示例：alpha_metrics
func (x *Transfer) StreamName(key string) string {
	return fmt.Sprintf(`%s_%s`, x.Namespace, key)
}

// SubName 根据 key 生成 NATS 发布主题名称。
//
// 格式：{namespace}.{key}
// 示例：alpha.metrics
func (x *Transfer) SubName(key string) string {
	return fmt.Sprintf(`%s.%s`, x.Namespace, key)
}

// Option 定义流配置选项。
type Option struct {
	// Key 流的唯一标识符，用于生成流名称和主题
	Key string `json:"key"`
	// Subs 额外订阅的主题列表（可选）
	// 允许流接收来自其他主题的消息
	Subs []string `json:"subs"`
	// Description 流的描述信息
	Description string `json:"description"`
	// Collection MongoDB 目标集合名称
	// 为空时使用 Key 作为集合名称
	Collection string `json:"collection"`
	// State 收集器运行时状态（仅 Get 方法返回）
	*State
}

// State 表示收集器的运行时状态。
type State struct {
	// BufferSize 收集器缓冲区中待写入的消息数量
	BufferSize int `json:"buffer_size"`
}

// Get 获取流配置和收集器状态。
//
// 执行流程：
//  1. 从 KV 读取流配置
//  2. 向收集器发送状态查询请求
//  3. 合并配置和状态返回
//
// 如果收集器未运行或未订阅该流，状态查询会超时。
func (x *Transfer) Get(ctx context.Context, key string) (option *Option, err error) {
	// 从 KV 获取配置
	var entry jetstream.KeyValueEntry
	if entry, err = x.Kv.Get(ctx, key); err != nil {
		return
	}
	if err = sonic.Unmarshal(entry.Value(), &option); err != nil {
		return
	}

	// 向收集器请求状态
	var msg *nats.Msg
	if msg, err = x.Nc.Request(fmt.Sprintf(`%s.states`, x.Namespace), []byte(key), 15*time.Second); err != nil {
		return
	}
	if err = sonic.Unmarshal(msg.Data, &option.State); err != nil {
		return
	}
	return
}

// Add 创建或更新流并将配置持久化到 KV。
//
// 执行流程：
//  1. 创建/更新 JetStream 流（WorkQueue 保留策略）
//  2. 创建/更新 durable 消费者（显式 ACK 策略）
//  3. 将配置写入 KV
//
// 收集器通过监听 KV 变更自动订阅新流。
//
// 流配置：
//   - Name: {namespace}_{key}
//   - Subjects: [{namespace}.{key}, ...option.Subs]
//   - Retention: WorkQueuePolicy（消息 ACK 后删除）
//
// 消费者配置：
//   - Durable: "default"
//   - AckPolicy: AckExplicitPolicy（需要显式 ACK）
func (x *Transfer) Add(ctx context.Context, option Option) (err error) {
	// 构建订阅主题列表
	subjects := []string{x.SubName(option.Key)}
	for _, sub := range option.Subs {
		subjects = append(subjects, sub)
	}

	// 创建/更新流
	var stream jetstream.Stream
	if stream, err = x.Js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        x.StreamName(option.Key),
		Subjects:    subjects,
		Description: option.Description,
		Retention:   jetstream.WorkQueuePolicy,
	}); err != nil {
		return
	}

	// 创建/更新消费者
	if _, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "default",
		AckPolicy: jetstream.AckExplicitPolicy,
	}); err != nil {
		return
	}

	// 将配置写入 KV
	// 收集器监听 KV 变更，会自动订阅此流
	var b []byte
	if b, err = sonic.Marshal(option); err != nil {
		return
	}
	if _, err = x.Kv.Put(ctx, option.Key, b); err != nil {
		return
	}
	return
}

// Send 发布 BSON 编码的数据到指定流。
//
// 数据会被序列化为 BSON 格式后发布到 {namespace}.{key} 主题。
// 使用异步发布，调用返回不代表消息已持久化。
//
// 如需确认发布完成，可使用：
//
//	<-t.Js.PublishAsyncComplete()
func (x *Transfer) Send(key string, data any) (err error) {
	// 序列化为 BSON
	var content []byte
	if content, err = bson.Marshal(data); err != nil {
		return
	}
	// 异步发布到 JetStream
	if _, err = x.Js.PublishAsync(x.SubName(key), content); err != nil {
		return
	}
	return
}

// Remove 从 KV 删除流配置。
//
// 收集器监听 KV 变更，会自动：
//  1. 停止收集器
//  2. 删除 JetStream 流
//
// 注意：这会删除流中所有未消费的消息。
func (x *Transfer) Remove(ctx context.Context, key string) (err error) {
	return x.Kv.Delete(ctx, key)
}
