package common

type Values struct {
	Mode        string   `yaml:"mode"`
	Namespace   string   `yaml:"namespace"`
	Description string   `yaml:"description"`
	Nats        Nats     `yaml:"nats"`
	Database    Database `yaml:"database"`
}

type Nats struct {
	Hosts []string `yaml:"hosts"`
	Token string   `yaml:"token"`
}

type Database struct {
	Url  string `yaml:"url"`
	Name string `yaml:"name"`
}
