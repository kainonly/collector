package mq

import "elastic-collector/app/types"

func (c *MessageQueue) Subscribe(option types.PipeOption) (err error) {
	if c.Drive == "amqp" {
		err = c.subscribeFormAmqp(option)
		if err != nil {
			return
		}
	}
	return
}

func (c *MessageQueue) subscribeFormAmqp(option types.PipeOption) (err error) {
	session := c.Amqp
	err = session.SetChannel(option.Identity)
	if err != nil {
		return
	}
	err = session.SetConsume(option)
	if err != nil {
		return
	}
	return
}
