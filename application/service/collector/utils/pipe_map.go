package utils

import "elastic-collector/config/options"

type PipeMap struct {
	hashMap map[string]*options.PipeOption
}

func NewPipeMap() *PipeMap {
	c := new(PipeMap)
	c.hashMap = make(map[string]*options.PipeOption)
	return c
}

func (c *PipeMap) Put(identity string, option *options.PipeOption) {
	c.hashMap[identity] = option
}

func (c *PipeMap) Empty(identity string) bool {
	return c.hashMap[identity] == nil
}

func (c *PipeMap) Get(identity string) *options.PipeOption {
	return c.hashMap[identity]
}

func (c *PipeMap) Lists() map[string]*options.PipeOption {
	return c.hashMap
}

func (c *PipeMap) Remove(identity string) {
	delete(c.hashMap, identity)
}
