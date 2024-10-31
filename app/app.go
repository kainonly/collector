package app

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weplanx/collector/v2/client"
	"github.com/weplanx/collector/v2/common"
	"go.uber.org/zap"
	"strings"
	"sync"
	"time"
)

type App struct {
	*common.Inject

	M sync.Map
}

type M = map[string]interface{}

type Payload struct {
	Timestamp time.Time              `msgpack:"timestamp"`
	Data      map[string]interface{} `msgpack:"data"`
	XData     map[string]interface{} `msgpack:"xdata"`
}

func Initialize(i *common.Inject) (x *App) {
	return &App{
		Inject: i,
		M:      sync.Map{},
	}
}

func (x *App) NamespaceParse() string {
	return strings.Replace(x.V.Namespace, "-", "_", -1)
}

func (x *App) name(key string) string {
	return fmt.Sprintf(`%s_%s`, x.NamespaceParse(), key)
}

func (x *App) subject(key string) string {
	return fmt.Sprintf(`%s.%s`, x.NamespaceParse(), key)
}

func (x *App) Get(key string) *nats.Subscription {
	if value, ok := x.M.Load(key); ok {
		return value.(*nats.Subscription)
	}
	return nil
}

func (x *App) Set(key string, v *nats.Subscription) {
	x.M.Store(key, v)
}

func (x *App) Remove(key string) {
	x.M.Delete(key)
}

func (x *App) Run() (err error) {
	var keys []string

	if keys, err = x.Kv.Keys(); errors.Is(err, nats.ErrNoObjectsFound) {
		if errors.Is(err, nats.ErrNoObjectsFound) {
			keys = make([]string, 0)
		} else {
			return
		}
	}
	for _, key := range keys {
		var entry nats.KeyValueEntry
		if entry, err = x.Kv.Get(key); err != nil {
			return
		}
		var option client.StreamOption
		if err = sonic.Unmarshal(entry.Value(), &option); err != nil {
			common.Log.Error("decoding fail",
				zap.ByteString("data", entry.Value()),
				zap.Error(err),
			)
			return
		}
		if err = x.SetSubscribe(key, &option); err != nil {
			common.Log.Error("subscription updated",
				zap.String("key", key),
				zap.String("subject", x.subject(option.Key)),
				zap.Error(err),
			)
		}
	}

	common.Log.Info("service started!")

	var watch nats.KeyWatcher
	if watch, err = x.Kv.WatchAll(); err != nil {
		return
	}
	cur := time.Now()
	for entry := range watch.Updates() {
		if entry == nil || entry.Created().Unix() < cur.Unix() {
			continue
		}
		switch entry.Operation().String() {
		case "KeyValuePutOp":
			var option client.StreamOption
			if err = sonic.Unmarshal(entry.Value(), &option); err != nil {
				common.Log.Error("decoding fail",
					zap.ByteString("data", entry.Value()),
					zap.Error(err),
				)
				return
			}
			time.Sleep(3 * time.Second)
			if err = x.SetSubscribe(entry.Key(), &option); err != nil {
				common.Log.Error("subscription updated",
					zap.String("key", entry.Key()),
					zap.String("subject", x.subject(option.Key)),
					zap.Error(err),
				)
			}
			break
		case "KeyValueDeleteOp":
			time.Sleep(3 * time.Second)
			if err = x.RemoveSubscribe(entry.Key()); err != nil {
				common.Log.Error("subscription removed",
					zap.String("key", entry.Key()),
					zap.Error(err),
				)
			}
			break
		}
	}

	return
}

func (x *App) SetSubscribe(key string, option *client.StreamOption) (err error) {
	var sub *nats.Subscription
	if sub, err = x.Js.QueueSubscribe(x.subject(key), x.name(key), func(msg *nats.Msg) {
		if err = x.Push(key, msg); err != nil {
			common.Log.Error("push fail",
				zap.Any("data", msg.Data),
				zap.Error(err),
			)
		}
	}, nats.ManualAck()); err != nil {
		return
	}
	x.Set(key, sub)
	common.Log.Debug("subscription updated",
		zap.String("key", key),
		zap.String("subject", x.subject(option.Key)),
	)
	return
}

func (x *App) RemoveSubscribe(key string) (err error) {
	if err = x.Get(key).Drain(); err != nil {
		return
	}
	x.Remove(key)
	common.Log.Debug("subscription removed",
		zap.String("key", key),
	)
	return
}

func (x *App) Push(key string, msg *nats.Msg) (err error) {
	var payload Payload
	if err = msgpack.Unmarshal(msg.Data, &payload); err != nil {
		common.Log.Error("decoding fail",
			zap.String("subject", msg.Subject),
			zap.String("data", string(msg.Data)),
			zap.Error(err),
		)
		return
	}
	common.Log.Debug("decoding",
		zap.String("subject", msg.Subject),
		zap.Any("data", payload),
	)
	data := payload.Data
	data["@timestamp"] = payload.Timestamp
	if err = x.Transform(data, payload.XData); err != nil {
		b, _ := sonic.Marshal(data)
		if _, err = x.Es.Index(fmt.Sprintf(`%s_fail`, key), bytes.NewReader(b)); err != nil {
			msg.NakWithDelay(time.Minute * 30)
			return
		}
		msg.Term()
		return
	}
	b, _ := sonic.Marshal(data)
	if _, err = x.Es.Index(key, bytes.NewReader(b)); err != nil {
		msg.NakWithDelay(time.Minute * 30)
		return
	}

	common.Log.Debug("push ok",
		zap.Any("payload", payload),
	)
	msg.Ack()
	return
}

func (x *App) Transform(data M, rules M) (err error) {
	for key, value := range rules {
		paths := strings.Split(key, ".")
		if err = x.Pipe(data, paths, value); err != nil {
			return
		}
	}
	return
}

func (x *App) Pipe(input M, paths []string, kind interface{}) (err error) {
	var cursor interface{} = input
	n := len(paths) - 1
	for i, path := range paths[:n] {
		if path == "$" {
			for _, item := range cursor.([]interface{}) {
				if err = x.Pipe(item.(M), paths[i+1:], kind); err != nil {
					return
				}
			}
			return
		}
		if cursor.(M)[path] == nil {
			return
		}
		cursor = cursor.(M)[path]
	}
	key := paths[n]
	if cursor == nil || cursor.(M)[key] == nil || cursor.(M)[key] == "" {
		return
	}
	unknow := cursor.(M)[key]
	var data interface{}
	switch kind {
	case "date":
		if v, ok := unknow.(string); ok {
			if data, err = time.Parse(time.RFC1123, v); err != nil {
				return
			}
		}
		break
	case "dates":
		if dates, ok := unknow.([]interface{}); ok {
			for i, date := range dates {
				if dates[i], err = time.Parse(time.RFC1123, date.(string)); err != nil {
					return
				}
			}
			data = dates
		}
		break
	case "timestamp":
		if v, ok := unknow.(string); ok {
			if data, err = time.Parse(time.RFC3339, v); err != nil {
				return
			}
		}
		break
	case "timestamps":
		if timestamps, ok := unknow.([]interface{}); ok {
			for i, timestamp := range timestamps {
				if timestamps[i], err = time.Parse(time.RFC3339, timestamp.(string)); err != nil {
					return
				}
			}
			data = timestamps
		}
		break
	case "json":
		if b, ok := unknow.([]byte); ok {
			if err = sonic.Unmarshal(b, &data); err != nil {
				return
			}
		}
		break
	}
	cursor.(M)[key] = data
	return
}
