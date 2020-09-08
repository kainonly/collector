package schema

type Schema struct {
	path string
}

func New() *Schema {
	c := new(Schema)
	c.path = "./config/autoload/"
	return c
}

func (c *Schema) autoload(identity string) string {
	return c.path + identity + ".yml"
}
