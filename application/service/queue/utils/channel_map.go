package utils

import (
	"github.com/streadway/amqp"
	"sync"
)

type ChannelMap struct {
	sync.RWMutex
	Map map[string]*amqp.Channel
}

func NewChannelMap() *ChannelMap {
	c := new(ChannelMap)
	c.Map = make(map[string]*amqp.Channel)
	return c
}

func (c *ChannelMap) Get(identity string) *amqp.Channel {
	c.RLock()
	value := c.Map[identity]
	c.RUnlock()
	return value
}

func (c *ChannelMap) Set(identity string, channel *amqp.Channel) {
	c.Lock()
	c.Map[identity] = channel
	c.Unlock()
}
