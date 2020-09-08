package manage

func (c *ElasticManager) Delete(identity string) (err error) {
	if c.pipes[identity] == nil {
		return
	}
	err = c.mq.Unsubscribe(identity)
	if err != nil {
		return
	}
	delete(c.pipes, identity)
	return c.schema.Delete(identity)
}
