package app

import (
	"context"
	"sync"
	"time"

	"github.com/kainonly/collector/v3/common"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// Collector 负责单个流的消息消费和批量写入。
//
// 工作机制：
//  1. 从 JetStream 消费者接收消息
//  2. 将消息累积到缓冲区
//  3. 触发刷新的条件（满足任一）：
//     - 缓冲区达到 BatchSize
//     - 定时器到达 FlushInterval
//     - 收到停止信号
//  4. 批量写入 MongoDB
//  5. 成功则 ACK，失败则 NAK 以便重试
type Collector struct {
	// app 父级 App 实例，提供配置和数据库连接
	app *App
	// option 流配置
	option Option
	// cc JetStream 消费上下文，用于停止消费
	cc jetstream.ConsumeContext

	// mu 保护 buffer 的互斥锁
	mu sync.Mutex
	// buffer 消息缓冲区
	buffer []jetstream.Msg
	// stopCh 停止信号通道
	stopCh chan struct{}
}

// NewCollector 创建新的 Collector 实例。
//
// 缓冲区预分配 BatchSize 容量以减少内存分配。
func NewCollector(app *App, option Option) *Collector {
	return &Collector{
		app:    app,
		option: option,
		buffer: make([]jetstream.Msg, 0, app.V.BatchSize),
		stopCh: make(chan struct{}),
	}
}

// Start 开始从 JetStream 消费者接收消息。
//
// 启动两个并发任务：
//   - 消息接收回调：将消息推入缓冲区
//   - 定时刷新循环：按 FlushInterval 定期刷新
func (c *Collector) Start(consumer jetstream.Consumer) (err error) {
	// 使用 push-based 消费模式
	if c.cc, err = consumer.Consume(func(msg jetstream.Msg) {
		c.push(msg)
	}); err != nil {
		return
	}
	// 启动定时刷新循环
	go c.flushLoop()
	return
}

// Stop 停止消费并执行最后一次刷新。
//
// 调用此方法后：
//  1. 停止从 JetStream 接收新消息
//  2. 通知 flushLoop 退出
//  3. flushLoop 在退出前会刷新缓冲区中的剩余消息
func (c *Collector) Stop() {
	if c.cc != nil {
		c.cc.Stop()
	}
	close(c.stopCh)
}

// BufferSize 返回当前缓冲区中的消息数量。
//
// 此方法是线程安全的，用于状态查询接口。
func (c *Collector) BufferSize() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.buffer)
}

// push 将消息添加到缓冲区。
//
// 如果缓冲区达到 BatchSize，立即触发刷新。
// 此方法由 JetStream 消费回调调用。
func (c *Collector) push(msg jetstream.Msg) {
	c.mu.Lock()
	c.buffer = append(c.buffer, msg)
	shouldFlush := len(c.buffer) >= c.app.V.BatchSize
	c.mu.Unlock()

	if shouldFlush {
		c.flush()
	}
}

// flushLoop 定时刷新循环。
//
// 按 FlushInterval 定期触发刷新，确保消息不会
// 在缓冲区中停留过久。
//
// 收到停止信号时，会先执行最后一次刷新再退出，
// 确保数据不丢失。
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

// flush 将缓冲区中的消息批量写入 MongoDB。
//
// 执行流程：
//  1. 获取锁，取出所有缓冲消息，重置缓冲区
//  2. 释放锁（允许新消息继续入队）
//  3. 提取消息数据（BSON 格式）
//  4. 批量插入 MongoDB
//  5. 成功：ACK 所有消息
//  6. 失败：NAK 所有消息（触发重新投递）
//
// 消息数据直接以原始 BSON 格式写入，无需反序列化。
func (c *Collector) flush() {
	// 快速获取并清空缓冲区
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

	// 提取消息数据（原始 BSON 字节）
	documents := make([]any, len(msgs))
	for i, msg := range msgs {
		documents[i] = msg.Data()
	}

	// 确定目标集合名称
	name := c.option.Key
	if c.option.Collection != "" {
		name = c.option.Collection
	}

	// 批量写入 MongoDB
	if _, err := c.app.Db.Collection(name).InsertMany(ctx, documents); err != nil {
		common.Log.Error("刷新失败",
			zap.String("key", c.option.Key),
			zap.Int("count", len(msgs)),
			zap.Error(err),
		)
		// NAK 所有消息，触发重新投递
		for _, msg := range msgs {
			_ = msg.Nak()
		}
		return
	}

	// ACK 所有消息，从流中移除
	for _, msg := range msgs {
		_ = msg.Ack()
	}

	common.Log.Info("刷新成功",
		zap.String("key", c.option.Key),
		zap.Int("count", len(msgs)),
	)
}
