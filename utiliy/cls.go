package utiliy

import (
	"github.com/nats-io/nats.go"
	cls "github.com/tencentcloud/tencentcloud-cls-sdk-go"
	"go.uber.org/zap"
)

type CLSReply struct {
	Logger *zap.Logger
	Msg    *nats.Msg
}

func (x *CLSReply) Success(result *cls.Result) {
	x.Msg.Ack()
	x.Logger.Debug("日志写入成功",
		zap.Any("attempts", result.GetReservedAttempts()),
	)
}

func (x *CLSReply) Fail(result *cls.Result) {
	x.Msg.Nak()
	x.Logger.Debug("日志写入失败",
		zap.String("request_id", result.GetRequestId()),
		zap.String("code", result.GetErrorCode()),
		zap.String("msg", result.GetErrorMessage()),
	)
}
