package utils

import (
	"sync"
)

type SyncChannelReady struct {
	sync.RWMutex
	Map map[string]bool
}

func NewSyncChannelReady() *SyncChannelReady {
	c := new(SyncChannelReady)
	c.Map = make(map[string]bool)
	return c
}

func (c *SyncChannelReady) Get(identity string) bool {
	c.RLock()
	value := c.Map[identity]
	c.RUnlock()
	return value
}

func (c *SyncChannelReady) Set(identity string, ready bool) {
	c.Lock()
	c.Map[identity] = ready
	c.Unlock()
}
