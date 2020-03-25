package elastic

import (
	"bytes"
	"errors"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
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

func (c *Elastic) Index(index string, data []byte) (err error) {
	var res *esapi.Response
	res, err = c.client.Index(
		index,
		bytes.NewBuffer(data),
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
