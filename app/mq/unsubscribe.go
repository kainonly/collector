package mq

func (c *MessageQueue) Unsubscribe(identity string) (err error) {
	if c.Drive == "amqp" {
		err = c.unsubscribeFormAmqp(identity)
		if err != nil {
			return
		}
	}
	return
}

func (c *MessageQueue) unsubscribeFormAmqp(identity string) (err error) {
	session := c.Amqp
	err = session.CloseChannel(identity)
	if err != nil {
		return
	}
	return
}
