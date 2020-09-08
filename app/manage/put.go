package manage

import "elastic-collector/app/types"

func (c *ElasticManager) Put(option types.PipeOption) (err error) {
	if c.pipes[option.Identity] != nil {
		err = c.mq.Unsubscribe(option.Identity)
		if err != nil {
			return
		}
	}
	err = c.mq.Subscribe(option)
	if err != nil {
		return
	}
	c.pipes[option.Identity] = &option
	return c.schema.Update(option)
}
