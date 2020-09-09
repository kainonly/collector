package utils

import (
	"sync"
)

type SyncChannelDone struct {
	sync.RWMutex
	Map map[string]chan int
}

func NewSyncChannelDone() *SyncChannelDone {
	c := new(SyncChannelDone)
	c.Map = make(map[string]chan int)
	return c
}

func (c *SyncChannelDone) Get(identity string) chan int {
	c.RLock()
	value := c.Map[identity]
	c.RUnlock()
	return value
}

func (c *SyncChannelDone) Set(identity string, done chan int) {
	c.Lock()
	c.Map[identity] = done
	c.Unlock()
}
