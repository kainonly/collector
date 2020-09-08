package manage

import "elastic-collector/app/types"

func (c *ElasticManager) Put(option types.PipeOption) (err error) {
	err = c.mq.Subscribe(option)
	if err != nil {
		return
	}
	c.pipes[option.Identity] = &option
	return c.schema.Update(option)
}
