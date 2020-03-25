package elastic

import (
	"bytes"
	"errors"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
)

type Elastic struct {
	client *elasticsearch.Client
}

func Create(config elasticsearch.Config) *Elastic {
	var err error
	elastic := new(Elastic)
	elastic.client, err = elasticsearch.NewClient(config)
	if err != nil {
		log.Fatalln(err)
	}
	return elastic
}

func (c *Elastic) Index(index string, data interface{}) (err error) {
	var buf bytes.Buffer
	var res *esapi.Response
	err = jsoniter.NewEncoder(&buf).Encode(data)
	if err != nil {
		return
	}
	res, err = c.client.Index(
		index,
		&buf,
	)
	if err != nil {
		return
	}
	defer res.Body.Close()
	if res.IsError() {
		return errors.New(res.Status())
	}
	return
}
