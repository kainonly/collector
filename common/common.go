package common

type Values struct {
	Nats Nats `yaml:"nats"`
}

type Nats struct {
	Hosts []string `yaml:"hosts"`
	Nkey  string   `yaml:"nkey"`
}
