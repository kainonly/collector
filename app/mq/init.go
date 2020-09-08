package mq

import (
	"elastic-collector/app/schema"
	"elastic-collector/app/types"
)

type MessageQueue struct {
	schema *schema.Schema
	types.MqOption
	Amqp AmqpDrive
}

func NewMessageQueue(option types.MqOption, schema *schema.Schema) (mq *MessageQueue, err error) {
	mq = new(MessageQueue)
	mq.MqOption = option
	mq.schema = schema
	if mq.Drive == "amqp" {
	}
	return
}
