package main

import (
	"github.com/weplanx/collector/bootstrap"
	"github.com/weplanx/collector/common"
	"go.uber.org/zap"
	"os"
	"os/signal"
)

func main() {
	var err error
	if common.Log, err = SetZap(); err != nil {
		panic(err)
	}
	app, err := bootstrap.NewApp()
	if err != nil {
		panic(err)
	}
	if err = app.Run(); err != nil {
		panic(err)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func SetZap() (log *zap.Logger, err error) {
	if os.Getenv("MODE") != "release" {
		if log, err = zap.NewDevelopment(); err != nil {
			return
		}
	} else {
		if log, err = zap.NewProduction(); err != nil {
			return
		}
	}
	return
}
