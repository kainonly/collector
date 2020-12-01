package main

import (
	"elastic-collector/application"
	"elastic-collector/bootstrap"
	"go.uber.org/fx"
)

func main() {
	fx.New(
		fx.NopLogger,
		fx.Provide(
			bootstrap.LoadConfiguration,
			bootstrap.InitializeSchema,
			bootstrap.InitializeQueue,
			bootstrap.InitializeElastic,
			bootstrap.InitializeCollector,
		),
		fx.Invoke(application.Application),
	).Run()
}
