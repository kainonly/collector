package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/go-co-op/gocron/v2"
	"github.com/kainonly/collector/v3/common"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type App struct {
	*common.Inject

	*M[string, gocron.Job]
}

type M[K comparable, S any] struct {
	m sync.Map
}

func (x *M[K, S]) LinkJob(key K, v S) {
	x.m.Store(key, v)
}

func (x *M[K, S]) Job(key K) S {
	if value, ok := x.m.Load(key); ok {
		return value.(S)
	}
	var zero S
	return zero
}

func (x *M[K, S]) UnLinkJob(key K) {
	x.m.Delete(key)
}

// Initialize constructs an App with injected dependencies.
func Initialize(i *common.Inject) (x *App) {
	return &App{
		Inject: i,
		M:      &M[string, gocron.Job]{m: sync.Map{}},
	}
}

type Option struct {
	Key         string   `json:"key"`
	Subs        []string `json:"subs"`
	Collection  string   `json:"collection"`
	Description string   `json:"description"`
}

func (x *App) StreamName(key string) string {
	return fmt.Sprintf(`%s_%s`, x.V.Namespace, key)
}

func (x *App) SubName(key string) string {
	return fmt.Sprintf(`%s.%s`, x.V.Namespace, key)
}

// Run loads all stream configs from KV, schedules jobs, and watches KV changes.
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
			common.Log.Error("decoding fail",
				zap.String("key", key),
				zap.Error(err),
			)
			return
		}
		if err = x.Subscribe(option); err != nil {
			common.Log.Error("subscribe fail",
				zap.String("key", key),
				zap.Error(err),
			)
		}
	}
	x.Schedule.Start()
	common.Log.Info(`service initialized successfully.`)

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
				common.Log.Error("decoding fail",
					zap.ByteString("data", entry.Value()),
					zap.Error(err),
				)
				return
			}
			if err = x.Subscribe(option); err != nil {
				common.Log.Error("subscribe fail",
					zap.String("key", key),
					zap.Error(err),
				)
			}
			break
		case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
			if err = x.Unsubscribe(key); err != nil {
				common.Log.Error("unsubscribe fail",
					zap.String("key", key),
					zap.Error(err),
				)
			}
			break
		}
	}

	return
}

// Subscribe schedules a periodic task for a key using its stream's default consumer.
func (x *App) Subscribe(option Option) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var consumer jetstream.Consumer
	if consumer, err = x.Js.Consumer(ctx, x.StreamName(option.Key), `default`); err != nil {
		return
	}

	var job gocron.Job
	if job, err = x.Schedule.NewJob(
		gocron.DurationJob(x.V.Duration),
		gocron.NewTask(func(o Option, c jetstream.Consumer) {
			if errX := x.Task(o, c); errX != nil {
				common.Log.Error("task fail",
					zap.String("key", o.Key),
					zap.Error(errX),
				)
			}
		}, option, consumer),
		gocron.WithTags(option.Key),
	); err != nil {
		return
	}

	x.LinkJob(option.Key, job)
	common.Log.Info("subscribe ok",
		zap.String("key", option.Key),
		zap.String("subject", x.SubName(option.Key)),
	)
	return
}

// Task fetches up to Batch messages and inserts them into MongoDB, ACKing on success.
func (x *App) Task(option Option, consumer jetstream.Consumer) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var msgBatch jetstream.MessageBatch
	if msgBatch, err = consumer.FetchNoWait(x.V.Batch); err != nil {
		return
	}

	documents := make([]any, 0)
	msgs := make([]jetstream.Msg, 0)
	for msg := range msgBatch.Messages() {
		documents = append(documents, msg.Data())
		msgs = append(msgs, msg)
	}

	if len(documents) == 0 {
		common.Log.Debug("task ok",
			zap.String("key", option.Key),
			zap.Int("documents", len(documents)),
		)
		return
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

	common.Log.Info("task ok",
		zap.String("key", option.Key),
		zap.Int("documents", len(documents)),
	)
	return
}

// Unsubscribe deletes the stream and removes its scheduled job.
func (x *App) Unsubscribe(key string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = x.Js.DeleteStream(ctx, x.StreamName(key)); err != nil {
		return
	}
	x.UnLinkJob(key)
	x.Schedule.RemoveByTags(key)
	common.Log.Info("unsubscribe ok",
		zap.String("key", key),
	)
	return
}

// States registers a request/reply endpoint for querying scheduled job state.
func (x *App) States() (err error) {
	if _, err = x.Nc.Subscribe(fmt.Sprintf(`%s.states`, x.V.Namespace), func(m *nats.Msg) {
		key := string(m.Data)
		b, errX := x.LoadState(key)
		if errX != nil {
			common.Log.Error("load state fail",
				zap.String("key", key),
				zap.Error(errX),
			)
			return
		}

		if errX = x.Nc.Publish(m.Reply, b); errX != nil {
			common.Log.Error("publish state fail",
				zap.String("key", key),
				zap.Error(errX),
			)
		}
	}); err != nil {
		return
	}
	return
}

type State struct {
	Nexts []time.Time `json:"nexts"`
	Last  time.Time   `json:"last"`
}

// LoadState returns next runs and last run time for a scheduled job.
func (x *App) LoadState(key string) (b []byte, err error) {
	job, state := x.Job(key), new(State)
	if state.Nexts, err = job.NextRuns(5); err != nil {
		return
	}
	if state.Last, err = job.LastRun(); err != nil {
		return
	}
	return sonic.Marshal(state)
}
