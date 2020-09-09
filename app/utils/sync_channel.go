package utils

import (
	"github.com/streadway/amqp"
	"sync"
)

type SyncChannel struct {
	sync.RWMutex
	Map map[string]*amqp.Channel
}

func NewSyncChannel() *SyncChannel {
	c := new(SyncChannel)
	c.Map = make(map[string]*amqp.Channel)
	return c
}

func (c *SyncChannel) Get(identity string) *amqp.Channel {
	c.RLock()
	value := c.Map[identity]
	c.RUnlock()
	return value
}

func (c *SyncChannel) Set(identity string, channel *amqp.Channel) {
	c.Lock()
	c.Map[identity] = channel
	c.Unlock()
}
