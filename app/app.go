package app

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
	cls "github.com/tencentcloud/tencentcloud-cls-sdk-go"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weplanx/collector/common"
	"go.uber.org/zap"
	"strconv"
	"time"
)

type App struct {
	*common.Inject
	*common.Collertor

	event *nats.Subscription
}

func (x *App) subject(topic string) string {
	return fmt.Sprintf(`%s.logs.%s`, x.Values.Namespace, topic)
}

func (x *App) queue(topic string) string {
	return fmt.Sprintf(`%s:logs:%s`, x.Values.Namespace, topic)
}

// Run 启动服务
func (x *App) Run() (err error) {
	namesapce := x.Values.Namespace
	// 初始化日志主题
	readySubject := fmt.Sprintf(`%s.logs`, namesapce)
	if _, err = x.Js.Subscribe(readySubject, func(msg *nats.Msg) {
		defer msg.Sub.Unsubscribe()
		var topics []string
		if err = msgpack.Unmarshal(msg.Data, &topics); err != nil {
			x.Log.Error("解码失败",
				zap.ByteString("data", msg.Data),
				zap.Error(err),
			)
			return
		}
		for _, v := range topics {
			if err = x.SetSubscribe(v); err != nil {
				x.Log.Error("订阅设置失败",
					zap.String("subject", x.subject(v)),
					zap.Error(err),
				)
			}
		}
		x.Log.Info("完成初始化订阅",
			zap.Any("topics", topics),
		)
	}); err != nil {
		return
	}

	// 订阅事件状态
	eventSubject := fmt.Sprintf(`%s.logs.events`, namesapce)
	if x.event, err = x.Js.Subscribe(eventSubject, func(msg *nats.Msg) {
		var event map[string]string
		if err = msgpack.Unmarshal(msg.Data, &event); err != nil {
			x.Log.Error("解码失败",
				zap.ByteString("data", msg.Data),
				zap.Error(err),
			)
			return
		}
		topic := event["topic"]
		x.Log.Info("事件",
			zap.String("topic", topic),
			zap.String("action", event["action"]),
		)
		switch event["action"] {
		case "create":
			if err := x.SetSubscribe(topic); err != nil {
				x.Log.Error("订阅设置失败",
					zap.String("subject", x.subject(topic)),
				)
			}
			break
		case "delete":
			if err := x.RemoveSubscribe(topic); err != nil {
				x.Log.Error("订阅移除失败",
					zap.String("subject", x.subject(topic)),
				)
			}
			break
		}

	}); err != nil {
		return
	}

	// 启动 CLS
	x.CLS.Start()
	return
}

// SetSubscribe 订阅设置
func (x *App) SetSubscribe(topic string) (err error) {
	var sub *nats.Subscription
	if sub, err = x.Js.QueueSubscribe(x.subject(topic), x.queue(topic), func(msg *nats.Msg) {
		var values map[string]interface{}
		if err := msgpack.Unmarshal(msg.Data, &values); err != nil {
			x.Log.Error("解码失败",
				zap.String("subject", msg.Subject),
				zap.ByteString("data", msg.Data),
				zap.Error(err),
			)
			return
		}
		data := make(map[string]string, len(values))
		for key, value := range values {
			switch v := value.(type) {
			case string:
				data[key] = v
				break
			case int64:
				data[key] = strconv.FormatInt(v, 10)
				break
			case float64:
				data[key] = fmt.Sprintf("%f", v)
				break
			default:
				json, err := jsoniter.Marshal(value)
				if err != nil {
					x.Log.Error("编码失败",
						zap.String("key", key),
						zap.Any("value", value),
						zap.Error(err),
					)
					return
				}
				data[key] = string(json)
			}
		}

		x.Log.Debug("解码成功",
			zap.String("subject", x.subject(topic)),
			zap.Any("data", data),
		)
		clog := cls.NewCLSLog(
			time.Now().Unix(),
			data,
		)
		reply := &common.CLSReply{Inject: x.Inject, Msg: msg}
		if err = x.CLS.SendLog(x.Values.CLS.TopicId, clog, reply); err != nil {
			x.Log.Error("日志写入失败",
				zap.Any("data", data),
				zap.Error(err),
			)
			return
		}
	}, nats.ManualAck()); err != nil {
		return
	}
	x.Set(topic, sub)
	x.Log.Info("订阅设置成功",
		zap.String("subject", x.subject(topic)),
	)
	return
}

func (x *App) Destory() (err error) {
	x.Log.Info("正在销毁...")
	x.CLS.Close(0)
	for _, v := range x.Value() {
		if err = v.Drain(); err != nil {
			return
		}
	}
	if err = x.Nats.Drain(); err != nil {
		return
	}
	return
}

// RemoveSubscribe 订阅移除
func (x *App) RemoveSubscribe(topic string) (err error) {
	if err = x.Get(topic).Drain(); err != nil {
		return
	}
	x.Remove(topic)
	x.Log.Info("订阅移除成功",
		zap.String("subject", x.subject(topic)),
	)
	return
}
