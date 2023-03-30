package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weplanx/collector/common"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"strings"
	"time"
)

type App struct {
	*common.Inject

	options map[string]*LogOption
	subs    map[string]*nats.Subscription
}

type LogOption struct {
	Key         string `msgpack:"key"`
	Description string `msgpack:"description"`
}

func Initialize(i *common.Inject) (x *App) {
	return &App{
		Inject:  i,
		options: make(map[string]*LogOption),
		subs:    make(map[string]*nats.Subscription),
	}
}

func (x *App) SubjectName(key string) string {
	return fmt.Sprintf(`%s.logs.%s`, x.Values.Namespace, key)
}

func (x *App) QueueName(key string) string {
	return fmt.Sprintf(`%s:logs:%s`, x.Values.Namespace, key)
}

func (x *App) Get(key string) *nats.Subscription {
	return x.subs[key]
}

func (x *App) Set(key string, option *LogOption, v *nats.Subscription) {
	x.options[key] = option
	x.subs[key] = v
}

func (x *App) Remove(key string) {
	delete(x.options, key)
	delete(x.subs, key)
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
		var option LogOption
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
			var option LogOption
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
			if err := x.RemoveSubscribe(entry.Key()); err != nil {
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

func (x *App) SetSubscribe(key string, option *LogOption) (err error) {
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

type Payload struct {
	Timestamp time.Time              `msgpack:"timestamp"`
	Data      map[string]interface{} `msgpack:"data"`
	Format    map[string]interface{} `msgpack:"format"`
}

func (x *App) Push(ctx context.Context, key string, msg *nats.Msg) (err error) {
	var payload Payload
	if err = msgpack.Unmarshal(msg.Data, &payload); err != nil {
		return
	}
	x.Log.Debug("Decoding",
		zap.String("subject", msg.Subject),
		zap.Any("data", payload),
		zap.Error(err),
	)
	name := fmt.Sprintf(`%s_logs`, key)
	data := payload.Data
	if err = x.Transform(data, payload.Format); err != nil {
		msg.Nak()
		return
	}
	data["timestamp"] = payload.Timestamp

	if _, err = x.Db.Collection(name).
		InsertOne(ctx, data); err != nil {
		msg.Nak()
		return
	}
	x.Log.Debug("Push",
		zap.Any("payload", payload),
	)
	msg.Ack()
	return
}

func (x *App) Transform(data map[string]interface{}, format map[string]interface{}) (err error) {
	for path, spec := range format {
		keys, cursor := strings.Split(path, "."), data
		n := len(keys) - 1
		for _, key := range keys[:n] {
			if v, ok := cursor[key].(map[string]interface{}); ok {
				cursor = v
			}
		}
		key := keys[n]
		if cursor[key] == nil {
			continue
		}
		switch spec {
		case "oid":
			if cursor[key], err = primitive.ObjectIDFromHex(cursor[key].(string)); err != nil {
				return
			}
			break

		case "oids":
			oids := cursor[key].([]interface{})
			for i, id := range oids {
				if oids[i], err = primitive.ObjectIDFromHex(id.(string)); err != nil {
					return
				}
			}
			break

		case "date":
			if cursor[key], err = time.Parse(time.RFC1123, cursor[key].(string)); err != nil {
				return
			}
			break

		case "timestamp":
			if cursor[key], err = time.Parse(time.RFC3339, cursor[key].(string)); err != nil {
				return
			}
			break

		}
	}
	return
}
