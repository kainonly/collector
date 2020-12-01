package elastic

import (
	"bytes"
	"errors"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type Elastic struct {
	Client *elasticsearch.Client
}

func (c *Elastic) Push(index string, data []byte) (err error) {
	var res *esapi.Response
	res, err = c.Client.Index(
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
