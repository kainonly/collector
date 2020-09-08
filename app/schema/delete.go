package schema

import "os"

func (c *Schema) Delete(identity string) error {
	return os.Remove(c.autoload(identity))
}
