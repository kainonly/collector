package app

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/weplanx/collector/common"
	"github.com/weplanx/collector/ds"
)

type App struct {
	*common.Inject

	Ds      ds.DataSource
	options map[string]*Option
	subs    map[string]*nats.Subscription
}

type Option struct {
	Topic       string `msgpack:"topic"`
	Description string `msgpack:"description"`
}

func New(i *common.Inject) (x *App, err error) {
	x = &App{
		Inject:  i,
		options: make(map[string]*Option),
		subs:    make(map[string]*nats.Subscription),
	}
	if x.Ds, err = ds.New(i); err != nil {
		return
	}
	return
}

func (x *App) subject(topic string) string {
	return fmt.Sprintf(`%s.logs.%s`, x.Values.Namespace, topic)
}

func (x *App) queue(topic string) string {
	return fmt.Sprintf(`%s:logs:%s`, x.Values.Namespace, topic)
}

func (x *App) Get(key string) *nats.Subscription {
	return x.subs[key]
}

func (x *App) Set(key string, option *Option, v *nats.Subscription) {
	x.options[key] = option
	x.subs[key] = v
}

func (x *App) Remove(key string) {
	delete(x.options, key)
	delete(x.subs, key)
}
