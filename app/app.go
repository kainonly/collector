package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/nats-io/nats.go"
	"github.com/weplanx/collector/common"
	"go.uber.org/zap"
	"time"
)

type Option struct {
	// 指标
	Measurement string `json:"measurement"`
	// 描述
	Description string `json:"description"`
}

type App struct {
	*common.Inject

	options map[string]*Option
	subs    map[string]*nats.Subscription
}

// Initialize 初始化 App
func Initialize(i *common.Inject) (x *App) {
	return &App{
		Inject:  i,
		options: make(map[string]*Option),
		subs:    make(map[string]*nats.Subscription),
	}
}

// 队列主题名称
func (x *App) subject(measurement string) string {
	return fmt.Sprintf(`%s.logs.%s`, x.Values.Namespace, measurement)
}

// 队列名称
func (x *App) queue(measurement string) string {
	return fmt.Sprintf(`%s:logs:%s`, x.Values.Namespace, measurement)
}

// Get 获取订阅
func (x *App) Get(measurement string) *nats.Subscription {
	return x.subs[measurement]
}

// Set 设置订阅配置
func (x *App) Set(measurement string, option *Option, v *nats.Subscription) {
	x.options[measurement] = option
	x.subs[measurement] = v
}

// Remove 移除订阅配置
func (x *App) Remove(measurement string) {
	delete(x.options, measurement)
	delete(x.subs, measurement)
}

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
		measurement := o.Name
		var b []byte
		if b, err = x.Store.GetBytes(measurement); err != nil {
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
		if err = x.SetSubscribe(measurement, &option); err != nil {
			x.Log.Error("订阅设置失败",
				zap.String("measurement", measurement),
				zap.String("subject", x.subject(option.Measurement)),
				zap.Error(err),
			)
		}
	}

	x.Log.Info("服务已启动")

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
		measurement := o.Name
		if !o.Deleted {
			var b []byte
			if b, err = x.Store.GetBytes(measurement); err != nil {
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
			if err := x.SetSubscribe(measurement, &option); err != nil {
				x.Log.Error("订阅设置失败",
					zap.String("measurement", measurement),
					zap.String("subject", x.subject(option.Measurement)),
					zap.Error(err),
				)
			}
		} else {
			if err := x.RemoveSubscribe(measurement); err != nil {
				x.Log.Error("订阅移除失败",
					zap.String("measurement", measurement),
					zap.Error(err),
				)
			}
		}
	}

	return
}

// SetSubscribe 订阅设置
func (x *App) SetSubscribe(measurement string, option *Option) (err error) {
	var sub *nats.Subscription
	if sub, err = x.Js.QueueSubscribe(x.subject(measurement), x.queue(measurement), func(msg *nats.Msg) {
		if err = x.Push(measurement, msg); err != nil {
			x.Log.Error("日志写入失败",
				zap.Any("data", msg.Data),
				zap.Error(err),
			)
		}
	}, nats.ManualAck()); err != nil {
		return
	}
	x.Set(measurement, option, sub)
	x.Log.Info("订阅设置成功",
		zap.String("measurement", measurement),
		zap.String("subject", x.subject(option.Measurement)),
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
	// 标签
	Tags map[string]string `json:"tags"`
	// 字段
	Fields map[string]interface{} `json:"fields"`
	// 时间
	Time time.Time `json:"time"`
}

// Push 推送日志
func (x *App) Push(measurement string, msg *nats.Msg) (err error) {
	var payload Payload
	if err = sonic.Unmarshal(msg.Data, &payload); err != nil {
		return
	}
	x.Log.Debug("解码成功",
		zap.String("subject", msg.Subject),
		zap.Any("data", payload),
		zap.Error(err),
	)
	api := x.Influx.WriteAPIBlocking(x.Values.Influx.Org, x.Values.Namespace)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	if err = api.WritePoint(ctx, influxdb2.NewPoint(
		measurement,
		payload.Tags,
		payload.Fields,
		payload.Time,
	)); err != nil {
		msg.Nak()
		return
	}
	x.Log.Debug("日志写入成功",
		zap.Any("payload", payload),
	)
	msg.Ack()
	return
}
