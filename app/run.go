package app

import (
	"context"
	"errors"
	"github.com/bytedance/sonic"
	"github.com/go-co-op/gocron/v2"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/weplanx/collector/v3/common"
	"go.uber.org/zap"
	"time"
)

func (x *App) Run(ctx context.Context) (err error) {
	var keys []string
	if keys, err = x.Kv.Keys(ctx); errors.Is(err, jetstream.ErrNoObjectsFound) {
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
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

	var watch jetstream.KeyWatcher
	if watch, err = x.Kv.WatchAll(ctx); err != nil {
		return
	}

	common.Log.Info(`automatically observing configuration changes.`)
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
				common.Log.Error("option decoding fail",
					zap.ByteString("data", entry.Value()),
					zap.Error(err),
				)
				return
			}

			if err = x.Subscribe(option); err != nil {
				common.Log.Error("subscription create faild",
					zap.String("key", key),
					zap.String("subject", x.SubName(key)),
					zap.Error(err),
				)
			}
			break
		case jetstream.KeyValueDelete:
		case jetstream.KeyValuePurge:
			if err = x.Unsubscribe(key); err != nil {

			}
			break
		}
	}

	return
}

func (x *App) Subscribe(option Option) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var consumer jetstream.Consumer
	if consumer, err = x.Js.Consumer(ctx, option.Key, `default`); err != nil {
		return
	}

	if _, err = x.Schedule.NewJob(
		gocron.DurationJob(5*time.Second),
		gocron.NewTask(x.Task, option),
		gocron.WithTags(option.Key),
		gocron.WithContext(ctx),
	); err != nil {
		return
	}

	x.Create(option.Key, consumer)
	common.Log.Debug("create ok",
		zap.String("key", option.Key),
		zap.String("subject", x.SubName(option.Key)),
	)
	return
}

func (x *App) Task(option Option, consumer jetstream.Consumer) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var msgBatch jetstream.MessageBatch
	if msgBatch, err = consumer.FetchNoWait(5000); err != nil {
		return
	}

	documents := make([]any, 0)
	msgs := make([]jetstream.Msg, 0)
	for msg := range msgBatch.Messages() {
		documents = append(documents, msg.Data())
		msgs = append(msgs, msg)
	}

	name := option.Key
	if option.Collection != "" {
		name = option.Collection
	}
	if _, err = x.Db.Collection(name).InsertMany(ctx, documents); err != nil {
		return
	}

	for _, msg := range msgs {
		msg.Ack()
	}

	return
}

func (x *App) Unsubscribe(key string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = x.Js.DeleteStream(ctx, x.StreamName(key)); err != nil {
		return
	}
	x.Remove(key)
	x.Schedule.RemoveByTags(key)
	common.Log.Debug("destroy ok",
		zap.String("key", key),
	)
	return
}
