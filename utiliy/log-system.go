package utiliy

import (
	"github.com/nats-io/nats.go"
	cls "github.com/tencentcloud/tencentcloud-cls-sdk-go"
	"github.com/weplanx/collector/common"
	"go.uber.org/zap"
	"time"
)

type LogSystem struct {
	Logger *zap.Logger

	Option map[string]interface{}
	Client interface{}
}

func NewLogSystem(v common.LogSystem, log *zap.Logger) (x *LogSystem, err error) {
	x = &LogSystem{
		Logger: log,
		Option: v.Option,
	}
	switch v.Type {
	case "cls":
		var client *cls.AsyncProducerClient
		if client, err = common.SetCLS(v.Option); err != nil {
			return
		}
		client.Start()
		x.Client = client
		break
	}
	return
}

func (x *LogSystem) Push(msg *nats.Msg, data map[string]string) (err error) {
	switch client := x.Client.(type) {
	case *cls.AsyncProducerClient:
		clog := cls.NewCLSLog(
			time.Now().Unix(),
			data,
		)
		reply := &CLSReply{Logger: x.Logger, Msg: msg}
		if err = client.SendLog(x.Option["topicid"].(string), clog, reply); err != nil {
			x.Logger.Error("日志写入失败",
				zap.Any("data", data),
				zap.Error(err),
			)
			return
		}
		break
	}
	return
}
