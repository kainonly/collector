package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go"
	"github.com/weplanx/collector/common"
	"go.uber.org/zap"
	"time"
)

type App struct {
	*common.Inject

	options map[string]*LogOption
	subs    map[string]*nats.Subscription
}

type LogOption struct {
	// 日志标识
	Key string `json:"key"`
	// 描述
	Description string `json:"description"`
}

// Initialize 初始化 App
func Initialize(i *common.Inject) (x *App) {
	return &App{
		Inject:  i,
		options: make(map[string]*LogOption),
		subs:    make(map[string]*nats.Subscription),
	}
}

// 队列主题名称
func (x *App) subject(key string) string {
	return fmt.Sprintf(`%s.logs.%s`, x.Values.Namespace, key)
}

// 队列名称
func (x *App) queue(key string) string {
	return fmt.Sprintf(`%s:logs:%s`, x.Values.Namespace, key)
}

// Get 获取订阅
func (x *App) Get(key string) *nats.Subscription {
	return x.subs[key]
}

// Set 设置订阅配置
func (x *App) Set(key string, option *LogOption, v *nats.Subscription) {
	x.options[key] = option
	x.subs[key] = v
}

// Remove 移除订阅配置
func (x *App) Remove(key string) {
	delete(x.options, key)
	delete(x.subs, key)
}

// Run 启动服务
func (x *App) Run() (err error) {
	// 初始化日志主题
	var keys []string

	if keys, err = x.KeyValue.Keys(); errors.Is(err, nats.ErrNoObjectsFound) {
		if errors.Is(err, nats.ErrNoObjectsFound) {
			keys = make([]string, 0)
		} else {
			return
		}
	}
	for _, key := range keys {
		var entry nats.KeyValueEntry
		if entry, err = x.KeyValue.Get(key); err != nil {
			return
		}
		var option LogOption
		if err = sonic.Unmarshal(entry.Value(), &option); err != nil {
			x.Log.Error("解码失败",
				zap.ByteString("data", entry.Value()),
				zap.Error(err),
			)
			return
		}
		if err = x.SetSubscribe(key, &option); err != nil {
			x.Log.Error("订阅设置失败",
				zap.String("key", key),
				zap.String("subject", x.subject(option.Key)),
				zap.Error(err),
			)
		}
	}

	x.Log.Info("服务已启动")

	// 订阅事件状态
	var watch nats.KeyWatcher
	if watch, err = x.KeyValue.WatchAll(); err != nil {
		return
	}
	cur := time.Now()
	for entry := range watch.Updates() {
		if entry == nil || entry.Created().Unix() < cur.Unix() {
			continue
		}
		switch entry.Operation().String() {
		case "KeyValuePutOp":
			var option LogOption
			if err = sonic.Unmarshal(entry.Value(), &option); err != nil {
				x.Log.Error("解码失败",
					zap.ByteString("data", entry.Value()),
					zap.Error(err),
				)
				return
			}
			time.Sleep(3 * time.Second)
			if err := x.SetSubscribe(entry.Key(), &option); err != nil {
				x.Log.Error("订阅设置失败",
					zap.String("key", entry.Key()),
					zap.String("subject", x.subject(option.Key)),
					zap.Error(err),
				)
			}
			break
		case "KeyValueDeleteOp":
			time.Sleep(3 * time.Second)
			if err := x.RemoveSubscribe(entry.Key()); err != nil {
				x.Log.Error("订阅移除失败",
					zap.String("key", entry.Key()),
					zap.Error(err),
				)
			}
			break
		}
	}

	return
}

// SetSubscribe 订阅设置
func (x *App) SetSubscribe(key string, option *LogOption) (err error) {
	var sub *nats.Subscription
	if sub, err = x.JetStream.QueueSubscribe(x.subject(key), x.queue(key), func(msg *nats.Msg) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err = x.Push(ctx, key, msg); err != nil {
			x.Log.Error("日志写入失败",
				zap.Any("data", msg.Data),
				zap.Error(err),
			)
		}
	}, nats.ManualAck()); err != nil {
		return
	}
	x.Set(key, option, sub)
	x.Log.Info("订阅设置成功",
		zap.String("key", key),
		zap.String("subject", x.subject(option.Key)),
	)
	return
}

// RemoveSubscribe 订阅移除
func (x *App) RemoveSubscribe(measurement string) (err error) {
	if err = x.Get(measurement).Drain(); err != nil {
		return
	}
	x.Remove(measurement)
	x.Log.Info("订阅移除成功",
		zap.String("measurement", measurement),
	)
	return
}

type Payload struct {
	// 元数据
	Metadata map[string]interface{} `bson:"metadata" json:"metadata"`
	// 日志
	Data map[string]interface{} `bson:"data" json:"data"`
	// 时间
	Timestamp time.Time `bson:"timestamp" json:"timestamp"`
}

// Push 数据推送
func (x *App) Push(ctx context.Context, key string, msg *nats.Msg) (err error) {
	var payload Payload
	if err = sonic.Unmarshal(msg.Data, &payload); err != nil {
		return
	}
	x.Log.Debug("解码成功",
		zap.String("subject", msg.Subject),
		zap.Any("data", payload),
		zap.Error(err),
	)
	coll := fmt.Sprintf(`%s_logs`, key)
	if _, err = x.Db.Collection(coll).
		InsertOne(ctx, payload); err != nil {
		msg.Nak()
		return
	}
	x.Log.Debug("日志写入成功",
		zap.Any("payload", payload),
	)
	msg.Ack()
	return
}
