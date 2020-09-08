package mq

import (
	"elastic-collector/app/schema"
	"elastic-collector/app/types"
)

type MessageQueue struct {
	types.MqOption
	Amqp *AmqpDrive
}

func NewMessageQueue(option types.MqOption, schema *schema.Schema) (mq *MessageQueue, err error) {
	mq = new(MessageQueue)
	mq.MqOption = option
	if mq.Drive == "amqp" {
		mq.Amqp, err = NewAmqpDrive(mq.Url, schema)
		if err != nil {
			return
		}
	}
	return
}
