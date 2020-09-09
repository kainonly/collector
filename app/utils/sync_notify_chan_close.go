package utils

import (
	"github.com/streadway/amqp"
	"sync"
)

type SyncNotifyChanClose struct {
	sync.RWMutex
	Map map[string]chan *amqp.Error
}

func NewSyncNotifyChanClose() *SyncNotifyChanClose {
	c := new(SyncNotifyChanClose)
	c.Map = make(map[string]chan *amqp.Error)
	return c
}

func (c *SyncNotifyChanClose) Get(identity string) chan *amqp.Error {
	c.RLock()
	value := c.Map[identity]
	c.RUnlock()
	return value
}

func (c *SyncNotifyChanClose) Set(identity string, notify chan *amqp.Error) {
	c.Lock()
	c.Map[identity] = notify
	c.Unlock()
}
