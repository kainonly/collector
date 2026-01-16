//go:build wireinject

package bootstrap

import (
	"context"

	"github.com/goforj/wire"
	"github.com/kainonly/collector/v3/app"
)

func NewApp(ctx context.Context) (*app.App, error) {
	wire.Build(
		LoadStaticValues,
		UseNats,
		UseMongo,
		UseJetStream,
		UseKeyValue,
		UseDatabase,
		UseSchedule,
		NewInject,
		app.Initialize,
	)
	return nil, nil
}
