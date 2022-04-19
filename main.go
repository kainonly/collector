package main

import (
	"github.com/weplanx/collector/common"
	"os"
	"os/signal"
)

func main() {
	v, err := common.SetValues()
	if err != nil {
		panic(err)
	}
	app, err := App(v)
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
