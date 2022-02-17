package common

import (
	"github.com/nats-io/nats.go"
)

type Collertor struct {
	values map[string]*nats.Subscription
}

func NewCollertor() *Collertor {
	return &Collertor{
		values: make(map[string]*nats.Subscription),
	}
}

func (x *Collertor) Value() map[string]*nats.Subscription {
	return x.values
}

func (x *Collertor) Size() int {
	return len(x.values)
}

func (x *Collertor) Get(k string) *nats.Subscription {
	return x.values[k]
}

func (x *Collertor) Set(k string, v *nats.Subscription) {
	x.values[k] = v
}

func (x *Collertor) Remove(k string) {
	delete(x.values, k)
}
