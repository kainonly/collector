package utiliy

import (
	"github.com/go-playground/validator/v10"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
	"time"
)

type Influx struct {
	Client influxdb2.Client
	Org    string
	Bucket string
	Logger *zap.Logger
}

func NewInflux(option map[string]interface{}, logger *zap.Logger) (_ LogSystem, err error) {
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
	Measurement string                 `msgpack:"measurement" validate:"required"`
	Tags        map[string]string      `msgpack:"tags" validate:"required"`
	Fields      map[string]interface{} `msgpack:"fields" validate:"required"`
	Time        time.Time              `msgpack:"time" validate:"required"`
}

func (x *Influx) Push(msg *nats.Msg) (err error) {
	var data InfluxDto
	if err = msgpack.Unmarshal(msg.Data, &data); err != nil {
		return
	}
	if err = validator.New().Struct(&data); err != nil {
		msg.Term()
		return
	}
	x.Logger.Debug("解码成功",
		zap.String("subject", msg.Subject),
		zap.Any("data", data),
		zap.Error(err),
	)
	api := x.Client.WriteAPI(x.Org, x.Bucket)
	p := influxdb2.NewPointWithMeasurement(data.Measurement).
		SetTime(data.Time)
	for k, v := range data.Tags {
		p = p.AddTag(k, v)
	}
	for k, v := range data.Fields {
		p = p.AddField(k, v)
	}
	api.WritePoint(p)
	api.Flush()
	x.Logger.Debug("日志写入成功",
		zap.Any("data", data),
	)
	msg.Ack()
	return
}
