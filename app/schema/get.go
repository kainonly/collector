package schema

import (
	"elastic-collector/app/types"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
)

func (c *Schema) Get(identity string) (option types.PipeOption, err error) {
	_, err = os.Stat(c.path + identity + ".yml")
	if err != nil {
		return
	}
	var bytes []byte
	bytes, err = ioutil.ReadFile(c.path + identity + ".yml")
	if err != nil {
		return
	}
	err = yaml.Unmarshal(bytes, &option)
	if err != nil {
		return
	}
	return
}
