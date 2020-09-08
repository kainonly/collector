package mq

import (
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

func (c *MessageQueue) listenConn() {
	select {
	case <-c.Amqp.NotifyConnClose:
		logrus.Error("AMQP connection has been disconnected")
		c.reconnected()
	}
}

func (c *MessageQueue) reconnected() {
	count := 0
	for {
		time.Sleep(time.Second * 5)
		count++
		logrus.Info("Trying to reconnect:", count)
		conn, err := amqp.Dial(c.Url)
		if err != nil {
			logrus.Error(err)
			continue
		}
		c.Amqp.Conn = conn
		c.Amqp.NotifyConnClose = make(chan *amqp.Error)
		conn.NotifyClose(c.Amqp.NotifyConnClose)
		go c.listenConn()
		logrus.Info("Attempt to reconnect successfully")
		break
	}
}
