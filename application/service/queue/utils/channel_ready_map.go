package utils

import (
	"sync"
)

type ChannelReadyMap struct {
	sync.RWMutex
	Map map[string]bool
}

func NewChannelReadyMap() *ChannelReadyMap {
	c := new(ChannelReadyMap)
	c.Map = make(map[string]bool)
	return c
}

func (c *ChannelReadyMap) Get(identity string) bool {
	c.RLock()
	value := c.Map[identity]
	c.RUnlock()
	return value
}

func (c *ChannelReadyMap) Set(identity string, ready bool) {
	c.Lock()
	c.Map[identity] = ready
	c.Unlock()
}
