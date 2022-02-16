package app

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weplanx/collector/common"
	"go.uber.org/zap"
	"log"
	"strings"
	"time"
)

type App struct {
	*common.Inject
	*common.Collertor

	event *nats.Subscription
}

// Run 启动服务
func (x *App) Run() (err error) {
	namesapce := x.Values.Namespace
	// 初始化日志主题
	readySubject := fmt.Sprintf(`namespaces.%s.ready`, namesapce)
	if _, err = x.Js.Subscribe(readySubject, func(msg *nats.Msg) {
		defer msg.Sub.Unsubscribe()
		var topics []string
		if err = msgpack.Unmarshal(msg.Data, &topics); err != nil {
			x.Log.Error("解码失败",
				zap.ByteString("data", msg.Data),
				zap.Error(err),
			)
		}
		for _, v := range topics {
			if err = x.SetSubscribe(v); err != nil {
				x.Log.Error("订阅设置失败",
					zap.String("topic", v),
					zap.Error(err),
				)
			}
		}
		x.Log.Info("初始化订阅",
			zap.String("topics", strings.Join(topics, ",")),
		)
	}); err != nil {
		return
	}

	// 订阅事件状态
	eventSubject := fmt.Sprintf(`namespaces.%s.event`, namesapce)
	if x.event, err = x.Js.Subscribe(eventSubject, func(msg *nats.Msg) {
		var event map[string]string
		if err = msgpack.Unmarshal(msg.Data, &event); err != nil {
			x.Log.Error("解码失败",
				zap.ByteString("data", msg.Data),
				zap.Error(err),
			)
		}
		topic := event["topic"]
		x.Log.Info("事件",
			zap.String("topic", topic),
			zap.String("action", event["action"]),
		)
		switch event["action"] {
		case "create":
			x.SetSubscribe(topic)
			break
		case "update":
			x.Remove(topic)
			time.Sleep(500 * time.Millisecond)
			x.SetSubscribe(topic)
			break
		case "delete":
			x.Remove(topic)
			break
		}

	}); err != nil {
		return
	}
	return
}

// Destory 销毁
func (x *App) Destory() (err error) {
	return x.Nats.Drain()
}

// SetSubscribe 订阅设置
func (x *App) SetSubscribe(topic string) (err error) {
	subject := fmt.Sprintf(`%s.%s`, x.Values.Namespace, topic)
	var sub *nats.Subscription
	if sub, err = x.Js.Subscribe(subject, func(msg *nats.Msg) {
		var data map[string]interface{}
		if err := msgpack.Unmarshal(msg.Data, &data); err != nil {
			x.Log.Error("解码失败",
				zap.String("topic", msg.Subject),
				zap.ByteString("data", msg.Data),
				zap.Error(err),
			)
			return
		}
		log.Println("topic", subject, "data", data)
		msg.Ack()
	}, nats.ManualAck()); err != nil {
		return
	}
	x.Set(topic, sub)
	x.Log.Info("订阅设置成功",
		zap.String("topic", subject),
	)
	return
}

// RemoveSubscribe 订阅移除
func (x *App) RemoveSubscribe(topic string) (err error) {
	if err = x.Get(topic).Unsubscribe(); err != nil {
		return
	}
	x.Remove(topic)
	x.Log.Info("订阅移除成功",
		zap.String("topic", fmt.Sprintf(`%s.%s`, x.Values.Namespace, topic)),
	)
	return
}
