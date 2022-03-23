package utiliy

import (
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Influx struct {
	Client influxdb2.Client
	Org    string
	Bucket string
}

func NewInflux(option map[string]interface{}, logger *zap.Logger) (_ LogSystem, err error) {
	x := new(Influx)
	x.Client = influxdb2.NewClient(
		option["url"].(string),
		option["token"].(string),
	)
	return x, nil
}

func (x *Influx) Push(msg *nats.Msg) (err error) {
	//api := x.Client.WriteAPI(x.Org, x.Bucket)
	return
}
