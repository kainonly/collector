//go:build wireinject
// +build wireinject

package bootstrap

import (
	"github.com/google/wire"
	"github.com/kainonly/collector/v3/app"
	"github.com/kainonly/collector/v3/common"
)

func NewApp() (*app.App, error) {
	wire.Build(
		wire.Struct(new(common.Inject), "*"),
		LoadStaticValues,
		UseNats,
		UseJetStream,
		UseKeyValue,
		UseMongo,
		UseDatabase,
		UseSchedule,
		app.Initialize,
	)
	return &app.App{}, nil
}
