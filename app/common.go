package app

import (
	"fmt"
	"github.com/google/wire"
	"github.com/weplanx/collector/common"
	"github.com/weplanx/collector/utiliy"
)

var Provides = wire.NewSet(New)

type App struct {
	*common.Inject
	Collertor *utiliy.Collertor
	LogSystem utiliy.LogSystem
}

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

func (x *App) subject(topic string) string {
	return fmt.Sprintf(`%s.logs.%s`, x.Values.Namespace, topic)
}

func (x *App) queue(topic string) string {
	return fmt.Sprintf(`%s:logs:%s`, x.Values.Namespace, topic)
}
