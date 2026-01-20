package app

import (
	"context"
	"sync"
	"time"

	"github.com/kainonly/collector/v3/common"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// Collector 处理单个流的缓冲消息消费
type Collector struct {
	app    *App
	option Option
	cc     jetstream.ConsumeContext

	mu     sync.Mutex
	buffer []jetstream.Msg
	stopCh chan struct{}
}

// NewCollector 创建新的 Collector 实例
func NewCollector(app *App, option Option) *Collector {
	return &Collector{
		app:    app,
		option: option,
		buffer: make([]jetstream.Msg, 0, app.V.BatchSize),
		stopCh: make(chan struct{}),
	}
}

// Start 开始从消费者消费消息
func (c *Collector) Start(consumer jetstream.Consumer) (err error) {
	if c.cc, err = consumer.Consume(func(msg jetstream.Msg) {
		c.push(msg)
	}); err != nil {
		return
	}
	go c.flushLoop()
	return
}

// Stop 停止消费者并通知 flush 循环退出
func (c *Collector) Stop() {
	if c.cc != nil {
		c.cc.Stop()
	}
	close(c.stopCh)
}

// BufferSize 返回当前缓冲区大小
func (c *Collector) BufferSize() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.buffer)
}

// push 将消息添加到缓冲区，达到批量大小时触发 flush
func (c *Collector) push(msg jetstream.Msg) {
	c.mu.Lock()
	c.buffer = append(c.buffer, msg)
	shouldFlush := len(c.buffer) >= c.app.V.BatchSize
	c.mu.Unlock()

	if shouldFlush {
		c.flush()
	}
}

// flushLoop 定时刷新缓冲区
func (c *Collector) flushLoop() {
	ticker := time.NewTicker(c.app.V.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.flush()
		case <-c.stopCh:
			c.flush() // 停止前最后刷新一次
			return
		}
	}
}

// flush 将缓冲区消息写入 MongoDB
func (c *Collector) flush() {
	c.mu.Lock()
	if len(c.buffer) == 0 {
		c.mu.Unlock()
		return
	}
	msgs := c.buffer
	c.buffer = make([]jetstream.Msg, 0, c.app.V.BatchSize)
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	documents := make([]any, len(msgs))
	for i, msg := range msgs {
		documents[i] = msg.Data()
	}

	name := c.option.Key
	if c.option.Collection != "" {
		name = c.option.Collection
	}

	if _, err := c.app.Db.Collection(name).InsertMany(ctx, documents); err != nil {
		common.Log.Error("刷新失败",
			zap.String("key", c.option.Key),
			zap.Int("count", len(msgs)),
			zap.Error(err),
		)
		// NAK 所有消息以便重试
		for _, msg := range msgs {
			_ = msg.Nak()
		}
		return
	}

	// ACK 所有消息
	for _, msg := range msgs {
		_ = msg.Ack()
	}

	common.Log.Info("刷新成功",
		zap.String("key", c.option.Key),
		zap.Int("count", len(msgs)),
	)
}
