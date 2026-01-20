package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/kainonly/collector/v3/app"
	"github.com/kainonly/collector/v3/bootstrap"
	"github.com/kainonly/collector/v3/common"
)

func main() {
	var err error
	if common.Log, err = bootstrap.SetZap(); err != nil {
		panic(err)
	}

	values, err := bootstrap.LoadStaticValues()
	if err != nil {
		panic(err)
	}

	nc, err := bootstrap.UseNats(values)
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	js, err := bootstrap.UseJetStream(nc)
	if err != nil {
		panic(err)
	}

	kv, err := bootstrap.UseKeyValue(values, js)
	if err != nil {
		panic(err)
	}

	mc, err := bootstrap.UseMongo(values)
	if err != nil {
		panic(err)
	}
	defer mc.Disconnect(context.Background())

	db := bootstrap.UseDatabase(values, mc)

	ctx, cancel := context.WithCancel(context.Background())

	x := app.New(values, nc, js, kv, db)
	if err = x.States(); err != nil {
		panic(err)
	}

	go func() {
		if err := x.Run(ctx); err != nil {
			common.Log.Error(err.Error())
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	cancel()
	x.Close()
}
