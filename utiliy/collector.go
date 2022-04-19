package utiliy

import (
	"github.com/nats-io/nats.go"
)

type Collertor struct {
	options map[string]*CollertorOption
	subs    map[string]*nats.Subscription
}

type CollertorOption struct {
	Topic       string `msgpack:"topic"`
	Description string `msgpack:"description"`
}

func NewCollertor() *Collertor {
	return &Collertor{
		subs:    make(map[string]*nats.Subscription),
		options: make(map[string]*CollertorOption),
	}
}

func (x *Collertor) Get(key string) *nats.Subscription {
	return x.subs[key]
}

func (x *Collertor) Set(key string, option *CollertorOption, v *nats.Subscription) {
	x.options[key] = option
	x.subs[key] = v
}

func (x *Collertor) Remove(key string) {
	delete(x.options, key)
	delete(x.subs, key)
}
