package main

import (
	"context"
	"github.com/weplanx/collector/v3/bootstrap"
	"github.com/weplanx/collector/v3/common"
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
	if err = app.States(); err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err = app.Run(ctx); err != nil {
		panic(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
