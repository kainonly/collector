package bootstrap

import (
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/nats-io/nats.go"
	"github.com/weplanx/collector/v2/common"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	"os"
	"strings"
)

func SetZap() (log *zap.Logger, err error) {
	if os.Getenv("MODE") != "release" {
		if log, err = zap.NewDevelopment(); err != nil {
			return
		}
	} else {
		if log, err = zap.NewProduction(); err != nil {
			return
		}
	}
	return
}

func LoadStaticValues() (v *common.Values, err error) {
	v = new(common.Values)
	var b []byte
	if b, err = os.ReadFile("./config/values.yml"); err != nil {
		return
	}
	if err = yaml.Unmarshal(b, &v); err != nil {
		return
	}
	return
}

func UseElastic(values *common.Values) (es *elasticsearch.Client, err error) {
	if es, err = elasticsearch.NewClient(elasticsearch.Config{
		Addresses: values.Elastic.Hosts,
		Username:  values.Elastic.Username,
		Password:  values.Elastic.Password,
	}); err != nil {
		return
	}
	return
}

func UseNats(values *common.Values) (nc *nats.Conn, err error) {
	if nc, err = nats.Connect(
		strings.Join(values.Nats.Hosts, ","),
		nats.Token(values.Nats.Token),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
	); err != nil {
		return
	}
	return
}

func UseJetStream(nc *nats.Conn) (nats.JetStreamContext, error) {
	return nc.JetStream(nats.PublishAsyncMaxPending(256))
}

func UseKeyValue(js nats.JetStreamContext) (nats.KeyValue, error) {
	return js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:      "collector",
		Description: "Distribution lightly queue stream collect service",
	})
}
