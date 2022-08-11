package app

import (
	"errors"
	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"time"
)

// Run 启动服务
func (x *App) Run() (err error) {
	// 初始化日志主题
	var objects []*nats.ObjectInfo
	if objects, err = x.Store.List(); errors.Is(err, nats.ErrNoObjectsFound) {
		if errors.Is(err, nats.ErrNoObjectsFound) {
			objects = make([]*nats.ObjectInfo, 0)
		} else {
			return
		}
	}
	for _, o := range objects {
		key := o.Name
		var b []byte
		if b, err = x.Store.GetBytes(key); err != nil {
			return
		}
		var option Option
		if err = sonic.Unmarshal(b, &option); err != nil {
			x.Log.Error("解码失败",
				zap.ByteString("data", b),
				zap.Error(err),
			)
			return
		}
		if err = x.SetSubscribe(key, &option); err != nil {
			x.Log.Error("订阅设置失败",
				zap.String("key", key),
				zap.String("subject", x.subject(option.Topic)),
				zap.Error(err),
			)
		}
	}

	// 订阅事件状态
	var watch nats.ObjectWatcher
	if watch, err = x.Store.Watch(); err != nil {
		return
	}
	current := time.Now()
	for o := range watch.Updates() {
		if o == nil || o.ModTime.Unix() < current.Unix() {
			continue
		}
		key := o.Name
		if !o.Deleted {
			var b []byte
			if b, err = x.Store.GetBytes(key); err != nil {
				return
			}
			var option Option
			if err = sonic.Unmarshal(b, &option); err != nil {
				x.Log.Error("解码失败",
					zap.ByteString("data", b),
					zap.Error(err),
				)
				return
			}
			if err := x.SetSubscribe(key, &option); err != nil {
				x.Log.Error("订阅设置失败",
					zap.String("key", key),
					zap.String("subject", x.subject(option.Topic)),
					zap.Error(err),
				)
			}
		} else {
			if err := x.RemoveSubscribe(key); err != nil {
				x.Log.Error("订阅移除失败",
					zap.String("key", key),
					zap.Error(err),
				)
			}
		}
	}
	return
}

// SetSubscribe 订阅设置
func (x *App) SetSubscribe(key string, option *Option) (err error) {
	var sub *nats.Subscription
	if sub, err = x.Js.QueueSubscribe(x.subject(option.Topic), x.queue(option.Topic), func(msg *nats.Msg) {
		if err = x.Ds.Push(msg); err != nil {
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
		zap.String("subject", x.subject(option.Topic)),
	)
	return
}

// RemoveSubscribe 订阅移除
func (x *App) RemoveSubscribe(key string) (err error) {
	if err = x.Get(key).Drain(); err != nil {
		return
	}
	x.Remove(key)
	x.Log.Info("订阅移除成功",
		zap.String("key", key),
	)
	return
}
