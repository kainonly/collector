package collector

import (
	"elastic-collector/application/service/collector/utils"
	"elastic-collector/application/service/queue"
	"elastic-collector/application/service/schema"
	"elastic-collector/config/options"
	"errors"
	"go.uber.org/fx"
)

type Collector struct {
	Pipes *utils.PipeMap
	*Dependency
}

type Dependency struct {
	fx.In

	Schema *schema.Schema
	Queue  *queue.Queue
}

var (
	NotExists = errors.New("this identity does not exists")
)

func New(dep *Dependency) (c *Collector, err error) {
	c = new(Collector)
	c.Dependency = dep
	c.Pipes = utils.NewPipeMap()
	var pipesOptions []options.PipeOption
	if pipesOptions, err = c.Schema.Lists(); err != nil {
		return
	}
	for _, option := range pipesOptions {
		if err = c.Put(option); err != nil {
			return
		}
	}
	return
}

func (c *Collector) GetPipe(identity string) (*options.PipeOption, error) {
	if c.Pipes.Empty(identity) {
		return nil, NotExists
	}
	return c.Pipes.Get(identity), nil
}
