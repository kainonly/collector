package main

import (
	"github.com/weplanx/collector/bootstrap"
	"os"
	"os/signal"
)

func main() {
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
