package consumer

import (
	"elastic-queue-logger/common"
	"elastic-queue-logger/elastic"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"os"
)

type Consumer struct {
	conn    *amqp.Connection
	channel map[string]*amqp.Channel
	elastic *elastic.Elastic
}

func Bootstrap(uri string, elastic *elastic.Elastic) *Consumer {
	var err error
	consumer := new(Consumer)
	consumer.conn, err = amqp.Dial(uri)
	if err != nil {
		logrus.Fatalln(err)
	}
	consumer.channel = make(map[string]*amqp.Channel)
	consumer.elastic = elastic
	var configs []common.ConsumerOption
	configs, err = common.ListConsumerOption()
	if err != nil {
		logrus.Fatalln(err)
	}
	for _, opt := range configs {
		err = consumer.Subscriber(opt)
		if err != nil {
			logrus.Fatalln(err)
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
		option.Identity,
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
			logger := logrus.New()
			var file *os.File
			if common.OpenStorage() {
				file, err = common.LogFile(option.Identity)
				if err != nil {
					return
				}
				logger.SetOutput(file)
			}
			if jsoniter.Valid(d.Body) {
				err := c.elastic.Index(option.Index, d.Body)
				if err != nil {
					logger.Error("nack:", err)
					d.Nack(false, true)
				}
				logger.Info("ack:", string(d.Body))
				d.Ack(false)
			} else {
				logger.Error("reject:", string(d.Body))
				d.Reject(false)
			}
		}
	}()
	return
}
