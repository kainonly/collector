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

// App 是收集器服务的核心结构
type App struct {
	V  *common.Values
	Nc *nats.Conn
	Js jetstream.JetStream
	Kv jetstream.KeyValue
	Db *mongo.Database

	collectors sync.Map // key -> *Collector
	stopCh     chan struct{}
}

// New 创建 App 实例
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

// Option 是流配置选项
type Option struct {
	Key         string   `json:"key"`
	Subs        []string `json:"subs"`
	Collection  string   `json:"collection"`
	Description string   `json:"description"`
}

// StreamName 返回流名称
func (x *App) StreamName(key string) string {
	return fmt.Sprintf(`%s_%s`, x.V.Namespace, key)
}

// SubName 返回订阅主题名称
func (x *App) SubName(key string) string {
	return fmt.Sprintf(`%s.%s`, x.V.Namespace, key)
}

// Run 从 KV 加载所有流配置，启动收集器，并监听 KV 变更
func (x *App) Run(ctx context.Context) (err error) {
	var keys []string
	if keys, err = x.Kv.Keys(ctx); err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			keys = make([]string, 0)
		} else {
			return
		}
	}
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

	var watch jetstream.KeyWatcher
	if watch, err = x.Kv.WatchAll(ctx); err != nil {
		return
	}

	common.Log.Info(`正在监听配置变更`)
	cur := time.Now()
	for entry := range watch.Updates() {
		if entry == nil || entry.Created().Unix() < cur.Unix() {
			continue
		}
		key := entry.Key()
		switch entry.Operation() {
		case jetstream.KeyValuePut:
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

// Subscribe 创建 push-based 消费者并启动缓冲写入
func (x *App) Subscribe(option Option) (err error) {
	// 如果已存在，先停止旧的收集器
	if v, ok := x.collectors.Load(option.Key); ok {
		v.(*Collector).Stop()
		x.collectors.Delete(option.Key)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var consumer jetstream.Consumer
	if consumer, err = x.Js.Consumer(ctx, x.StreamName(option.Key), `default`); err != nil {
		return
	}

	collector := NewCollector(x, option)
	if err = collector.Start(consumer); err != nil {
		return
	}

	x.collectors.Store(option.Key, collector)
	common.Log.Info("订阅成功",
		zap.String("key", option.Key),
		zap.String("subject", x.SubName(option.Key)),
	)
	return
}

// Unsubscribe 停止收集器并删除流
func (x *App) Unsubscribe(key string) (err error) {
	if v, ok := x.collectors.Load(key); ok {
		v.(*Collector).Stop()
		x.collectors.Delete(key)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = x.Js.DeleteStream(ctx, x.StreamName(key)); err != nil {
		return
	}
	common.Log.Info("取消订阅成功",
		zap.String("key", key),
	)
	return
}

// Close 停止所有收集器
func (x *App) Close() {
	close(x.stopCh)
	x.collectors.Range(func(key, value any) bool {
		value.(*Collector).Stop()
		return true
	})
}
