package actions

import (
	"bytes"
	"errors"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

func Push(client *elasticsearch.Client, index string, data []byte) (err error) {
	var res *esapi.Response
	res, err = client.Index(
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
