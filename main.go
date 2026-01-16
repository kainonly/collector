package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/kainonly/collector/v3/bootstrap"
	"github.com/kainonly/collector/v3/common"
)

func main() {
	var err error
	if common.Log, err = bootstrap.SetZap(); err != nil {
		panic(err)
	}
	ctx := context.Background()
	app, err := bootstrap.NewApp(ctx)
	if err != nil {
		panic(err)
	}
	if err = app.States(); err != nil {
		panic(err)
	}

	if err = app.Run(ctx); err != nil {
		panic(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
