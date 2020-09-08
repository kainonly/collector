package manage

import "elastic-collector/app/types"

func (c *ElasticManager) Put(option types.PipeOption) (err error) {
	c.pipes[option.Identity] = &option
	return c.schema.Update(option)
}
