package common

import (
	"elastic-collector/application/service/collector"
	"elastic-collector/application/service/elastic"
	"elastic-collector/application/service/queue"
	"elastic-collector/application/service/schema"
	"elastic-collector/config"
	"go.uber.org/fx"
)

type Dependency struct {
	fx.In

	Config    *config.Config
	Schema    *schema.Schema
	Queue     *queue.Queue
	ES        *elastic.Elastic
	Collector *collector.Collector
}
