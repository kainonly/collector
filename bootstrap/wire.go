//go:build wireinject
// +build wireinject

package bootstrap

import (
	"github.com/google/wire"
	"github.com/weplanx/collector/v3/app"
	"github.com/weplanx/collector/v3/common"
)

func NewApp() (*app.App, error) {
	wire.Build(
		wire.Struct(new(common.Inject), "*"),
		LoadStaticValues,
		UseMongo,
		UseDatabase,
		UseNats,
		UseJetStream,
		UseKeyValue,
		app.Initialize,
	)
	return &app.App{}, nil
}
