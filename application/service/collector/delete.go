package collector

func (c *Collector) Delete(identity string) (err error) {
	if c.Pipes.Empty(identity) {
		return
	}
	if err = c.Queue.Unsubscribe(identity); err != nil {
		return
	}
	c.Pipes.Remove(identity)
	return c.Schema.Delete(identity)
}
