package bootstrap

import (
	"github.com/caarlos0/env/v10"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/nats-io/nats.go"
	"github.com/weplanx/collector/common"
	"strings"
)

func LoadStaticValues() (values *common.Values, err error) {
	values = new(common.Values)
	if err = env.Parse(values); err != nil {
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
