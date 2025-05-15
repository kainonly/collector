package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go"
	"github.com/weplanx/collector/v3/common"
	"go.uber.org/zap"
	"sync"
	"time"
)

type App struct {
	*common.Inject
	*M[string, *nats.Subscription]
}

type M[K comparable, S any] struct {
	m sync.Map
}

func (x *M[K, S]) Create(key K, v S) {
	x.m.Store(key, v)
}

func (x *M[K, S]) Destroy(key K) S {
	if value, ok := x.m.LoadAndDelete(key); ok {
		return value.(S)
	}
	var zero S
	return zero
}

func (x *M[K, S]) Remove(key K) {
	x.m.Delete(key)
}

func Initialize(i *common.Inject) (x *App) {
	return &App{
		Inject: i,
		M:      &M[string, *nats.Subscription]{m: sync.Map{}},
	}
}

type Option struct {
	Key         string   `json:"key"`
	Subs        []string `json:"subs"`
	Collection  string   `json:"collection"`
	Description string   `json:"description"`
}

func (x *App) Name(key string) string {
	return fmt.Sprintf(`%s_%s`, x.V.Namespace, key)
}

func (x *App) SubName(key string) string {
	return fmt.Sprintf(`%s.%s`, x.V.Namespace, key)
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
		var option Option
		if err = sonic.Unmarshal(entry.Value(), &option); err != nil {
			common.Log.Error("option decoding fail",
				zap.ByteString("data", entry.Value()),
				zap.Error(err),
			)
			return
		}
		if err = x.Subscribe(option); err != nil {
			common.Log.Error("subscription create fail",
				zap.String("key", key),
				zap.String("subject", x.SubName(key)),
				zap.Error(err),
			)
		}
	}
	common.Log.Info(`collector service has been initialized successfully.`)

	var watch nats.KeyWatcher
	if watch, err = x.Kv.WatchAll(); err != nil {
		return
	}

	common.Log.Info(`automatically observing configuration changes.`)
	cur := time.Now()
	for entry := range watch.Updates() {
		if entry == nil || entry.Created().Unix() < cur.Unix() {
			continue
		}
		key := entry.Key()
		switch entry.Operation().String() {
		case "KeyValuePutOp":
			var option Option
			if err = sonic.Unmarshal(entry.Value(), &option); err != nil {
				common.Log.Error("option decoding fail",
					zap.ByteString("data", entry.Value()),
					zap.Error(err),
				)
				return
			}

			time.Sleep(3 * time.Second)
			if err = x.Subscribe(option); err != nil {
				common.Log.Error("subscription create faild",
					zap.String("key", key),
					zap.String("subject", x.SubName(key)),
					zap.Error(err),
				)
			}
			break
		case "KeyValueDeleteOp":
			time.Sleep(3 * time.Second)
			if err = x.Unsubscribe(key); err != nil {
				common.Log.Error("subscription destroy failed",
					zap.String("key", key),
					zap.Error(err),
				)
			}
			break
		}
	}

	return
}

func (x *App) Subscribe(option Option) (err error) {
	var subscription *nats.Subscription
	if subscription, err = x.Js.QueueSubscribe(x.SubName(option.Key), x.Name(option.Key), func(msg *nats.Msg) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err = x.Factory(ctx, option, msg.Data); err != nil {
			msg.NakWithDelay(time.Minute * 30)
			common.Log.Error("push fail",
				zap.Any("data", msg.Data),
				zap.Error(err),
			)
		}
		msg.Ack()
	}, nats.ManualAck()); err != nil {
		return
	}

	x.Create(option.Key, subscription)
	common.Log.Debug("subscription create ok",
		zap.String("key", option.Key),
		zap.String("subject", x.SubName(option.Key)),
	)
	return
}

func (x *App) Factory(ctx context.Context, option Option, data []byte) (err error) {
	name := option.Key
	if option.Collection != "" {
		name = option.Collection
	}
	if _, err = x.Db.Collection(name).InsertOne(ctx, data); err != nil {
		return
	}
	return
}

func (x *App) Unsubscribe(key string) (err error) {
	if err = x.Destroy(key).Drain(); err != nil {
		return
	}
	common.Log.Debug("subscription destroy ok",
		zap.String("key", key),
	)
	return
}
