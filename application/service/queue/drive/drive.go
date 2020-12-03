package drive

import (
	"elastic-collector/application/service/elastic"
	"elastic-collector/application/service/schema"
	"elastic-collector/config/options"
	"errors"
	"go.uber.org/fx"
)

var (
	QueueNotExists = errors.New("available queue does not exist")
)

type Dependency struct {
	fx.In

	Schema *schema.Schema
	ES     *elastic.Elastic
}

type API interface {
	Subscribe(option options.PipeOption) (err error)
	Unsubscribe(identity string) (err error)
}
