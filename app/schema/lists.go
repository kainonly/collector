package schema

import (
	"elastic-collector/app/types"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"path/filepath"
)

func (c *Schema) Lists() (options []types.PipeOption, err error) {
	var files []os.FileInfo
	files, err = ioutil.ReadDir(c.path)
	if err != nil {
		return
	}
	for _, file := range files {
		ext := filepath.Ext(file.Name())
		if ext == ".yml" {
			var bytes []byte
			bytes, err = ioutil.ReadFile(c.path + file.Name())
			if err != nil {
				return
			}
			var option types.PipeOption
			err = yaml.Unmarshal(bytes, &option)
			if err != nil {
				return
			}
			options = append(options, option)
		}
	}
	return
}
