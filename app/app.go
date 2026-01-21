// Package app 提供收集器服务的核心运行时。
//
// 主要职责：
//   - 管理多个流的收集器实例
//   - 监听 KV 变更实现动态订阅/取消订阅
//   - 提供状态查询接口
package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/kainonly/collector/v3/common"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/zap"
)

// App 是收集器服务的核心结构，管理所有流的收集器实例。
type App struct {
	// V 应用配置
	V *common.Values
	// Nc NATS 连接
	Nc *nats.Conn
	// Js JetStream 上下文
	Js jetstream.JetStream
	// Kv 命名空间 KV 存储桶，存储流配置
	Kv jetstream.KeyValue
	// Db MongoDB 数据库句柄
	Db *mongo.Database

	// collectors 存储所有活跃的收集器实例
	// key: 流的 key，value: *Collector
	collectors sync.Map
	// stopCh 用于通知关闭
	stopCh chan struct{}
}

// New 创建 App 实例。
//
// 参数：
//   - v: 应用配置
//   - nc: NATS 连接
//   - js: JetStream 上下文
//   - kv: KV 存储桶
//   - db: MongoDB 数据库句柄
func New(
	v *common.Values,
	nc *nats.Conn,
	js jetstream.JetStream,
	kv jetstream.KeyValue,
	db *mongo.Database,
) *App {
	return &App{
		V:      v,
		Nc:     nc,
		Js:     js,
		Kv:     kv,
		Db:     db,
		stopCh: make(chan struct{}),
	}
}

// Option 定义流配置选项，存储在 KV 中。
type Option struct {
	// Key 流的唯一标识符
	Key string `json:"key"`
	// Subs 额外订阅的主题列表（可选）
	Subs []string `json:"subs"`
	// Collection MongoDB 集合名称，为空时使用 Key
	Collection string `json:"collection"`
	// Description 流的描述信息
	Description string `json:"description"`
}

// StreamName 根据 key 生成 JetStream 流名称。
//
// 格式：{namespace}_{key}
// 示例：alpha_metrics
func (x *App) StreamName(key string) string {
	return fmt.Sprintf(`%s_%s`, x.V.Namespace, key)
}

// SubName 根据 key 生成 NATS 订阅主题名称。
//
// 格式：{namespace}.{key}
// 示例：alpha.metrics
func (x *App) SubName(key string) string {
	return fmt.Sprintf(`%s.%s`, x.V.Namespace, key)
}

// Run 启动收集器服务的主运行循环。
//
// 执行流程：
//  1. 从 KV 加载所有现有流配置
//  2. 为每个配置创建并启动收集器
//  3. 监听 KV 变更：
//     - PUT：创建或更新收集器
//     - DELETE/PURGE：停止收集器并删除流
//
// 此方法会阻塞直到 ctx 被取消。
func (x *App) Run(ctx context.Context) (err error) {
	// 加载所有现有的流配置
	var keys []string
	if keys, err = x.Kv.Keys(ctx); err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			keys = make([]string, 0)
		} else {
			return
		}
	}

	// 为每个配置启动收集器
	for _, key := range keys {
		var entry jetstream.KeyValueEntry
		if entry, err = x.Kv.Get(ctx, key); err != nil {
			return
		}
		var option Option
		if err = sonic.Unmarshal(entry.Value(), &option); err != nil {
			common.Log.Error("解码失败",
				zap.String("key", key),
				zap.Error(err),
			)
			return
		}
		if err = x.Subscribe(option); err != nil {
			common.Log.Error("订阅失败",
				zap.String("key", key),
				zap.Error(err),
			)
		}
	}
	common.Log.Info(`服务初始化成功`)

	// 开始监听 KV 变更
	var watch jetstream.KeyWatcher
	if watch, err = x.Kv.WatchAll(ctx); err != nil {
		return
	}

	common.Log.Info(`正在监听配置变更`)
	// 记录当前时间，忽略历史事件
	cur := time.Now()
	for entry := range watch.Updates() {
		// 跳过空条目和历史条目
		if entry == nil || entry.Created().Unix() < cur.Unix() {
			continue
		}
		key := entry.Key()
		switch entry.Operation() {
		case jetstream.KeyValuePut:
			// 新增或更新配置，创建/重建收集器
			var option Option
			if err = sonic.Unmarshal(entry.Value(), &option); err != nil {
				common.Log.Error("解码失败",
					zap.ByteString("data", entry.Value()),
					zap.Error(err),
				)
				return
			}
			if err = x.Subscribe(option); err != nil {
				common.Log.Error("订阅失败",
					zap.String("key", key),
					zap.Error(err),
				)
			}
		case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
			// 删除配置，停止收集器并清理流
			if err = x.Unsubscribe(key); err != nil {
				common.Log.Error("取消订阅失败",
					zap.String("key", key),
					zap.Error(err),
				)
			}
		}
	}

	return
}

// Subscribe 为指定配置创建收集器并开始消费消息。
//
// 如果该 key 已有收集器在运行，会先停止旧的再创建新的。
// 这允许配置的热更新。
func (x *App) Subscribe(option Option) (err error) {
	// 如果已存在，先停止旧的收集器
	if v, ok := x.collectors.Load(option.Key); ok {
		v.(*Collector).Stop()
		x.collectors.Delete(option.Key)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 获取 JetStream 消费者
	var consumer jetstream.Consumer
	if consumer, err = x.Js.Consumer(ctx, x.StreamName(option.Key), `default`); err != nil {
		return
	}

	// 创建并启动收集器
	collector := NewCollector(x, option)
	if err = collector.Start(option.Key, consumer); err != nil {
		return
	}

	x.collectors.Store(option.Key, collector)
	common.Log.Info("订阅成功",
		zap.String("key", option.Key),
		zap.String("subject", x.SubName(option.Key)),
	)
	return
}

// Unsubscribe 停止收集器并删除 JetStream 流。
//
// 停止收集器时会先刷新缓冲区中的剩余消息。
func (x *App) Unsubscribe(key string) (err error) {
	// 停止收集器
	if v, ok := x.collectors.Load(key); ok {
		v.(*Collector).Stop()
		x.collectors.Delete(key)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 删除 JetStream 流
	if err = x.Js.DeleteStream(ctx, x.StreamName(key)); err != nil {
		return
	}
	common.Log.Info("取消订阅成功",
		zap.String("key", key),
	)
	return
}

// Close 优雅关闭所有收集器。
//
// 停止每个收集器时会刷新其缓冲区，确保数据不丢失。
func (x *App) Close() {
	close(x.stopCh)
	x.collectors.Range(func(key, value any) bool {
		value.(*Collector).Stop()
		return true
	})
}
