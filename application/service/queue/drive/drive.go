package drive

import (
	"elastic-collector/application/service/elastic"
	"elastic-collector/application/service/schema"
	"elastic-collector/config/options"
	"go.uber.org/fx"
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
