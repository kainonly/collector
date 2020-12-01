package collector

import (
	"elastic-collector/config/options"
)

func (c *Collector) Put(option options.PipeOption) (err error) {
	if !c.Pipes.Empty(option.Identity) {
		if err = c.Queue.Unsubscribe(option.Identity); err != nil {
			return
		}
	}
	if err = c.Queue.Subscribe(option); err != nil {
		return
	}
	c.Pipes.Put(option.Identity, &option)
	return c.Schema.Update(option)
}
