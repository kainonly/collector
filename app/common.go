package app

import (
	"github.com/google/wire"
	"github.com/weplanx/collector/common"
	"github.com/weplanx/collector/utiliy"
)

var Provides = wire.NewSet(New)

func New(i *common.Inject) (x *App, err error) {
	x = &App{
		Inject:    i,
		Collertor: utiliy.NewCollertor(),
	}
	if x.LogSystem, err = utiliy.NewLogSystem(i); err != nil {
		return
	}
	return
}
