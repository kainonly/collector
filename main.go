package main

import (
	"github.com/weplanx/collector/bootstrap"
	"github.com/weplanx/collector/common"
	"os"
	"os/signal"
)

func main() {
	var err error
	if common.Log, err = bootstrap.SetZap(); err != nil {
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
