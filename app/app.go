package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weplanx/collector/common"
	"github.com/weplanx/collector/transfer"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

func Initialize(i *common.Inject) (x *App) {
	return &App{
		Inject: i,
		M:      sync.Map{},
	}
}

func (x *App) SubjectName(key string) string {
	return fmt.Sprintf(`%s.logs.%s`, x.Values.Namespace, key)
}

func (x *App) QueueName(key string) string {
	return fmt.Sprintf(`%s:logs:%s`, x.Values.Namespace, key)
}

func (x *App) Get(key string) *nats.Subscription {
	if value, ok := x.M.Load(key); ok {
		return value.(*nats.Subscription)
	}
	return nil
}

func (x *App) Set(key string, option *transfer.StreamOption, v *nats.Subscription) {
	x.M.Store(key, v)
}

func (x *App) Remove(key string) {
	x.M.Delete(key)
}

func (x *App) Run() (err error) {
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
		var option transfer.StreamOption
		if err = msgpack.Unmarshal(entry.Value(), &option); err != nil {
			x.Log.Error("Decoding",
				zap.ByteString("data", entry.Value()),
				zap.Error(err),
			)
			return
		}
		if err = x.SetSubscribe(key, &option); err != nil {
			x.Log.Error("Subscription updated",
				zap.String("key", key),
				zap.String("subject", x.SubjectName(option.Key)),
				zap.Error(err),
			)
		}
	}

	x.Log.Info("Service started!")

	// Watch
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
			var option transfer.StreamOption
			if err = msgpack.Unmarshal(entry.Value(), &option); err != nil {
				x.Log.Error("Decoding",
					zap.ByteString("data", entry.Value()),
					zap.Error(err),
				)
				return
			}
			time.Sleep(3 * time.Second)
			if err := x.SetSubscribe(entry.Key(), &option); err != nil {
				x.Log.Error("Subscription updated",
					zap.String("key", entry.Key()),
					zap.String("subject", x.SubjectName(option.Key)),
					zap.Error(err),
				)
			}
			break
		case "KeyValueDeleteOp":
			time.Sleep(3 * time.Second)
			if err = x.RemoveSubscribe(entry.Key()); err != nil {
				x.Log.Error("Subscription removed",
					zap.String("key", entry.Key()),
					zap.Error(err),
				)
			}
			break
		}
	}

	return
}

func (x *App) SetSubscribe(key string, option *transfer.StreamOption) (err error) {
	var sub *nats.Subscription
	if sub, err = x.JetStream.QueueSubscribe(x.SubjectName(key), x.QueueName(key), func(msg *nats.Msg) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err = x.Push(ctx, key, msg); err != nil {
			x.Log.Error("Push",
				zap.Any("data", msg.Data),
				zap.Error(err),
			)
		}
	}, nats.ManualAck()); err != nil {
		return
	}
	x.Set(key, option, sub)
	x.Log.Debug("Subscription updated",
		zap.String("key", key),
		zap.String("subject", x.SubjectName(option.Key)),
	)
	return
}

func (x *App) RemoveSubscribe(measurement string) (err error) {
	if err = x.Get(measurement).Drain(); err != nil {
		return
	}
	x.Remove(measurement)
	x.Log.Debug("Subscription removed",
		zap.String("measurement", measurement),
	)
	return
}

func (x *App) Push(ctx context.Context, key string, msg *nats.Msg) (err error) {
	var payload transfer.Payload
	if err = msgpack.Unmarshal(msg.Data, &payload); err != nil {
		x.Log.Error("Decoding Failed",
			zap.String("subject", msg.Subject),
			zap.String("data", string(msg.Data)),
			zap.Error(err),
		)
		return
	}
	x.Log.Debug("Decoding",
		zap.String("subject", msg.Subject),
		zap.Any("data", payload),
	)
	data := payload.Data
	data["timestamp"] = payload.Timestamp
	if err = x.Transform(data, payload.XData); err != nil {
		if _, err = x.Db.Collection(fmt.Sprintf(`%s_fail`, key)).
			InsertOne(ctx, data); err != nil {
			msg.NakWithDelay(time.Minute * 30)
			return
		}
		msg.Ack()
		return
	}
	if _, err = x.Db.Collection(key).
		InsertOne(ctx, data); err != nil {
		msg.NakWithDelay(time.Minute * 30)
		return
	}
	x.Log.Debug("Push",
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
	case "oid":
		if data, err = primitive.ObjectIDFromHex(unknow.(string)); err != nil {
			return
		}
		break
	case "oids":
		oids := unknow.([]interface{})
		for i, id := range oids {
			if oids[i], err = primitive.ObjectIDFromHex(id.(string)); err != nil {
				return
			}
		}
		data = oids
		break
	case "date":
		if data, err = time.Parse(time.RFC1123, unknow.(string)); err != nil {
			return
		}
		break
	case "dates":
		dates := unknow.([]interface{})
		for i, date := range dates {
			if dates[i], err = time.Parse(time.RFC1123, date.(string)); err != nil {
				return
			}
		}
		data = dates
		break
	case "timestamp":
		if data, err = time.Parse(time.RFC3339, unknow.(string)); err != nil {
			return
		}
		break
	case "timestamps":
		timestamps := unknow.([]interface{})
		for i, timestamp := range timestamps {
			if timestamps[i], err = time.Parse(time.RFC3339, timestamp.(string)); err != nil {
				return
			}
		}
		data = timestamps
		break
	case "json":
		if err = sonic.Unmarshal(unknow.([]byte), &data); err != nil {
			return
		}
		break
	}
	cursor.(M)[key] = data
	return
}
