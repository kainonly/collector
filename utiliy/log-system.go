package utiliy

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/weplanx/collector/common"
)

type LogSystem interface {
	Push(msg *nats.Msg) (err error)
}

func NewLogSystem(i *common.Inject) (x LogSystem, err error) {
	v := i.Values.LogSystem
	switch v.Type {
	case "cls":
		if x, err = NewCLS(v.Option, i.Log); err != nil {
			return
		}
		break
	case "influx":
		if x, err = NewInflux(v.Option, i.Log); err != nil {
			return
		}
		break
	default:
		return nil, fmt.Errorf(`不存在 [%s] 日志系统类型`, v.Type)
	}
	return
}
