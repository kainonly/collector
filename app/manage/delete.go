package manage

func (c *ElasticManager) Delete(identity string) (err error) {
	if c.pipes[identity] == nil {
		return
	}
	delete(c.pipes, identity)
	return c.schema.Delete(identity)
}
