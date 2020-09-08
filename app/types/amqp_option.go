package types

import "github.com/streadway/amqp"

type AmqpOption struct {
	Conn            *amqp.Connection
	NotifyConnClose chan *amqp.Error
}
