package manage

import "elastic-transfer/app/actions"

func (c *ElasticManager) Push(identity string, data []byte) (err error) {
	if err = c.empty(identity); err != nil {
		return
	}
	pipe := c.pipes[identity]
	err = actions.Push(c.client, pipe.Index, data)
	if err != nil {
		err = c.mq.Push(pipe.Topic, pipe.Key, data)
		if err != nil {
			return
		}
		return nil
	}
	return
}
