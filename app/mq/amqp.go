package mq

import (
	"elastic-collector/app/actions"
	"elastic-collector/app/schema"
	"elastic-collector/app/types"
	"elastic-collector/app/utils"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

type AmqpDrive struct {
	url             string
	client          *elasticsearch.Client
	schema          *schema.Schema
	conn            *amqp.Connection
	notifyConnClose chan *amqp.Error
	channel         *utils.SyncChannel
	channelDone     *utils.SyncChannelDone
	notifyChanClose *utils.SyncNotifyChanClose
}

func NewAmqpDrive(url string, client *elasticsearch.Client, schema *schema.Schema) (session *AmqpDrive, err error) {
	session = new(AmqpDrive)
	session.url = url
	session.client = client
	session.schema = schema
	conn, err := amqp.Dial(url)
	if err != nil {
		return
	}
	session.conn = conn
	session.notifyConnClose = make(chan *amqp.Error)
	conn.NotifyClose(session.notifyConnClose)
	go session.listenConn()
	session.channel = utils.NewSyncChannel()
	session.channelDone = utils.NewSyncChannelDone()
	session.notifyChanClose = utils.NewSyncNotifyChanClose()
	return
}

func (c *AmqpDrive) listenConn() {
	select {
	case <-c.notifyConnClose:
		logrus.Error("AMQP connection has been disconnected")
		c.reconnected()
	}
}

func (c *AmqpDrive) reconnected() {
	count := 0
	for {
		time.Sleep(time.Second * 5)
		count++
		logrus.Info("Trying to reconnect:", count)
		conn, err := amqp.Dial(c.url)
		if err != nil {
			logrus.Error(err)
			continue
		}
		c.conn = conn
		c.notifyConnClose = make(chan *amqp.Error)
		conn.NotifyClose(c.notifyConnClose)
		go c.listenConn()
		// TODO:处理掉线自动恢复
		logrus.Info("Attempt to reconnect successfully")
		break
	}
}

func (c *AmqpDrive) SetChannel(ID string) (err error) {
	var channel *amqp.Channel
	channel, err = c.conn.Channel()
	if err != nil {
		return
	}
	c.channel.Set(ID, channel)
	c.channelDone.Set(ID, make(chan int))
	notifyChanClose := make(chan *amqp.Error)
	channel.NotifyClose(notifyChanClose)
	c.notifyChanClose.Set(ID, notifyChanClose)
	go c.listenChannel(ID)
	return
}

func (c *AmqpDrive) listenChannel(ID string) {
	select {
	case <-c.notifyChanClose.Get(ID):
		logrus.Error("Channel connection is disconnected:", ID)
		c.refreshChannel(ID)
	case <-c.channelDone.Get(ID):
		break
	}
}

func (c *AmqpDrive) refreshChannel(ID string) {
	for {
		err := c.SetChannel(ID)
		if err != nil {
			continue
		}
		option, err := c.schema.Get(ID)
		if err != nil {
			continue
		}
		err = c.SetConsume(option)
		if err != nil {
			continue
		}
		logrus.Info("Channel refresh successfully")
		break
	}
}

func (c *AmqpDrive) CloseChannel(ID string) error {
	c.channelDone.Get(ID) <- 1
	return c.channel.Get(ID).Close()
}

func (c *AmqpDrive) SetConsume(option types.PipeOption) (err error) {
	_, err = c.channel.Get(option.Identity).QueueDeclare(
		option.Queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return
	}
	msgs, err := c.channel.Get(option.Identity).Consume(
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
		for d := range msgs {
			err = actions.Push(c.client, option.Index, d.Body)
			if err != nil {
				time.Sleep(time.Second * 30)
				d.Nack(false, true)
			} else {
				d.Ack(false)
			}
		}
	}()
	return
}
