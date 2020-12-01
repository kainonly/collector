package utils

import (
	"sync"
)

type ChannelDoneMap struct {
	sync.RWMutex
	Map map[string]chan int
}

func NewChannelDoneMap() *ChannelDoneMap {
	c := new(ChannelDoneMap)
	c.Map = make(map[string]chan int)
	return c
}

func (c *ChannelDoneMap) Get(identity string) chan int {
	c.RLock()
	value := c.Map[identity]
	c.RUnlock()
	return value
}

func (c *ChannelDoneMap) Set(identity string, done chan int) {
	c.Lock()
	c.Map[identity] = done
	c.Unlock()
}
