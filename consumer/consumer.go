package consumer

import (
	"elastic-queue-logger/common"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Consumer struct {
	conn    *amqp.Connection
	channel map[string]*amqp.Channel
}

func Bootstrap(opt *common.AmqpOption) *Consumer {
	var err error
	consumer := new(Consumer)
	consumer.conn, err = amqp.Dial(
		"amqp://" + opt.Username + ":" + opt.Password + "@" + opt.Host + ":" + opt.Port + opt.Vhost,
	)
	if err != nil {
		log.Fatalln(err)
	}
	consumer.channel = make(map[string]*amqp.Channel)
	var configs []common.ConsumerOption
	configs, err = common.ListConsumerOption()
	if err != nil {
		log.Fatalln(err)
	}
	for _, opt := range configs {
		err = consumer.Subscriber(opt)
		if err != nil {
			log.Fatalln(err)
		}
	}
	return consumer
}

func (c *Consumer) Close() {
	c.conn.Close()
}

func (c *Consumer) Subscriber(option common.ConsumerOption) (err error) {
	c.channel[option.Identity], err = c.conn.Channel()
	if err != nil {
		return
	}
	delivery, err := c.channel[option.Identity].Consume(
		option.Queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return
	}
	go func() {
		for d := range delivery {
			log.Info(option.Identity)
			log.Info(string(d.Body))
			d.Ack(false)
		}
	}()
	return
}
