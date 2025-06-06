package app

import (
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/weplanx/collector/v3/common"
	"sync"
)

type App struct {
	*common.Inject

	*M[string, jetstream.Consumer]
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
		M:      &M[string, jetstream.Consumer]{m: sync.Map{}},
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
