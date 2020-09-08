package manage

import (
	"elastic-collector/app/mq"
	"elastic-collector/app/schema"
	"elastic-collector/app/types"
	"errors"
	"github.com/elastic/go-elasticsearch/v8"
)

type ElasticManager struct {
	client *elasticsearch.Client
	mq     *mq.MessageQueue
	pipes  map[string]*types.PipeOption
	schema *schema.Schema
}

func NewElasticManager(
	config elasticsearch.Config,
	mq *mq.MessageQueue,
	schema *schema.Schema,
) (manager *ElasticManager, err error) {
	manager = new(ElasticManager)
	manager.client, err = elasticsearch.NewClient(config)
	manager.mq = mq
	if err != nil {
		return
	}
	manager.pipes = make(map[string]*types.PipeOption)
	manager.schema = schema
	var pipesOptions []types.PipeOption
	pipesOptions, err = manager.schema.Lists()
	if err != nil {
		return
	}
	for _, option := range pipesOptions {
		err = manager.Put(option)
		if err != nil {
			return
		}
	}
	return
}

func (c *ElasticManager) empty(identity string) error {
	if c.pipes[identity] == nil {
		return errors.New("this identity does not exists")
	}
	return nil
}

func (c *ElasticManager) GetIdentityCollection() []string {
	var keys []string
	for key := range c.pipes {
		keys = append(keys, key)
	}
	return keys
}

func (c *ElasticManager) GetOption(identity string) (option *types.PipeOption, err error) {
	if err = c.empty(identity); err != nil {
		return
	}
	option = c.pipes[identity]
	return
}
