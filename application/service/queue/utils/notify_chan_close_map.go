package utils

import (
	"github.com/streadway/amqp"
	"sync"
)

type NotifyChanCloseMap struct {
	sync.RWMutex
	Map map[string]chan *amqp.Error
}

func NewNotifyChanCloseMap() *NotifyChanCloseMap {
	c := new(NotifyChanCloseMap)
	c.Map = make(map[string]chan *amqp.Error)
	return c
}

func (c *NotifyChanCloseMap) Get(identity string) chan *amqp.Error {
	c.RLock()
	value := c.Map[identity]
	c.RUnlock()
	return value
}

func (c *NotifyChanCloseMap) Set(identity string, notify chan *amqp.Error) {
	c.Lock()
	c.Map[identity] = notify
	c.Unlock()
}
