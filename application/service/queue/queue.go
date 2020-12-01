package queue

import (
	"elastic-collector/application/service/queue/drive"
	"elastic-collector/config/options"
)

type Queue struct {
	Drive interface{}
	drive.API
}

type Option struct {
	Drive  string                 `yaml:"drive"`
	Option map[string]interface{} `yaml:"option"`
}

func (c *Queue) Subscribe(option options.PipeOption) (err error) {
	return c.Drive.(drive.API).Subscribe(option)
}

func (c *Queue) Unsubscribe(identity string) (err error) {
	return c.Drive.(drive.API).Unsubscribe(identity)
}
