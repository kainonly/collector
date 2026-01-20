package app

import (
	"fmt"

	"github.com/bytedance/sonic"
	"github.com/kainonly/collector/v3/common"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// State 是收集器状态
type State struct {
	BufferSize int `json:"buffer_size"`
}

// States 注册请求/响应端点用于查询收集器状态
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

// LoadState 返回收集器的当前缓冲区大小
func (x *App) LoadState(key string) (b []byte, err error) {
	v, ok := x.collectors.Load(key)
	if !ok {
		return nil, fmt.Errorf("收集器未找到: %s", key)
	}
	collector := v.(*Collector)
	state := &State{BufferSize: collector.BufferSize()}
	return sonic.Marshal(state)
}
