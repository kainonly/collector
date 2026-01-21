package app

import (
	"fmt"

	"github.com/bytedance/sonic"
	"github.com/kainonly/collector/v3/common"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// State 表示收集器的运行时状态。
type State struct {
	// BufferSize 当前缓冲区中待写入的消息数量
	BufferSize int `json:"buffer_size"`
}

// States 注册 NATS 请求/响应端点，用于外部查询收集器状态。
//
// 订阅主题：{namespace}.states
// 请求数据：流的 key
// 响应数据：JSON 编码的 State 结构
//
// 此接口供 Transfer SDK 的 Get 方法使用，
// 允许生产者查询消息是否已被消费。
func (x *App) States() (err error) {
	if _, err = x.Nc.Subscribe(fmt.Sprintf(`%s.states`, x.V.Namespace), func(m *nats.Msg) {
		key := string(m.Data)
		b, errX := x.LoadState(key)
		if errX != nil {
			common.Log.Error("加载状态失败",
				zap.String("key", key),
				zap.Error(errX),
			)
			return
		}

		if errX = x.Nc.Publish(m.Reply, b); errX != nil {
			common.Log.Error("发布状态失败",
				zap.String("key", key),
				zap.Error(errX),
			)
		}
	}); err != nil {
		return
	}
	return
}

// LoadState 获取指定 key 收集器的当前状态。
//
// 返回 JSON 编码的 State 结构，包含当前缓冲区大小。
// 如果收集器不存在，返回错误。
func (x *App) LoadState(key string) (b []byte, err error) {
	v, ok := x.collectors.Load(key)
	if !ok {
		return nil, fmt.Errorf("收集器未找到: %s", key)
	}
	collector := v.(*Collector)
	state := &State{BufferSize: collector.BufferSize()}
	return sonic.Marshal(state)
}
