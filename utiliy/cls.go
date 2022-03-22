package utiliy

import (
	"github.com/nats-io/nats.go"
	cls "github.com/tencentcloud/tencentcloud-cls-sdk-go"
	"go.uber.org/zap"
	"time"
)

type CLS struct {
	Client  *cls.AsyncProducerClient
	TopicId string
	Logger  *zap.Logger
}

func NewCLS(option map[string]interface{}, logger *zap.Logger) (_ LogSystem, err error) {
	x := new(CLS)
	producerConfig := cls.GetDefaultAsyncProducerClientConfig()
	producerConfig.AccessKeyID = option["secret_id"].(string)
	producerConfig.AccessKeySecret = option["secret_key"].(string)
	producerConfig.Endpoint = option["endpoint"].(string)
	if x.Client, err = cls.NewAsyncProducerClient(producerConfig); err != nil {
		return
	}
	x.TopicId = option["topic_id"].(string)
	x.Client.Start()
	x.Logger = logger
	return x, nil
}

func (x *CLS) Push(msg *nats.Msg, data map[string]string) (err error) {
	clog := cls.NewCLSLog(
		time.Now().Unix(),
		data,
	)
	reply := &CLSReply{Logger: x.Logger, Msg: msg}
	if err = x.Client.SendLog(x.TopicId, clog, reply); err != nil {
		x.Logger.Error("日志写入失败",
			zap.Any("data", data),
			zap.Error(err),
		)
		return
	}
	return
}

type CLSReply struct {
	Msg    *nats.Msg
	Logger *zap.Logger
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
