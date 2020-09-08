package mq

import (
	"elastic-collector/app/schema"
	"elastic-collector/app/types"
	"github.com/elastic/go-elasticsearch/v8"
)

type MessageQueue struct {
	types.MqOption
	amqp *AmqpDrive
}

func NewMessageQueue(
	option types.MqOption,
	client *elasticsearch.Client,
	schema *schema.Schema,
) (mq *MessageQueue, err error) {
	mq = new(MessageQueue)
	mq.MqOption = option
	if mq.Drive == "amqp" {
		mq.amqp, err = NewAmqpDrive(mq.Url, client, schema)
		if err != nil {
			return
		}
	}
	return
}
