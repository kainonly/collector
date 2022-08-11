package ds

import (
	"context"
	"github.com/bytedance/sonic"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"time"
)

type Influx struct {
	Client influxdb2.Client
	Org    string
	Bucket string
	Logger *zap.Logger
}

func NewInflux(option map[string]interface{}, logger *zap.Logger) (_ DataSource, err error) {
	x := new(Influx)
	x.Client = influxdb2.NewClient(
		option["url"].(string),
		option["token"].(string),
	)
	x.Org = option["org"].(string)
	x.Bucket = option["bucket"].(string)
	x.Logger = logger
	return x, nil
}

type InfluxDto struct {
	Measurement string                 `json:"measurement"`
	Tags        map[string]string      `json:"tags"`
	Fields      map[string]interface{} `json:"fields"`
	Time        time.Time              `json:"time"`
}

func (x *Influx) Push(msg *nats.Msg) (err error) {
	var data InfluxDto
	if err = sonic.Unmarshal(msg.Data, &data); err != nil {
		return
	}
	x.Logger.Debug("解码成功",
		zap.String("subject", msg.Subject),
		zap.Any("data", data),
		zap.Error(err),
	)
	api := x.Client.WriteAPIBlocking(x.Org, x.Bucket)
	p := influxdb2.NewPointWithMeasurement(data.Measurement).SetTime(data.Time)
	for k, v := range data.Tags {
		p = p.AddTag(k, v)
	}
	for k, v := range data.Fields {
		p = p.AddField(k, v)
	}
	if err = api.WritePoint(context.TODO(), p); err != nil {
		msg.Nak()
		return
	}
	x.Logger.Debug("日志写入成功",
		zap.Any("data", data),
	)
	msg.Ack()
	return
}
