package app

import (
	"github.com/google/wire"
	"github.com/weplanx/collector/common"
	"github.com/weplanx/collector/utiliy"
)

var Provides = wire.NewSet(New)

func New(i *common.Inject) *App {
	return &App{
		Inject:    i,
		Collertor: utiliy.NewCollertor(),
	}
}
