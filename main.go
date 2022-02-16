package main

import (
	"github.com/weplanx/collector/bootstrap"
)

func main() {
	v, err := bootstrap.SetValues()
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
	defer app.Destory()
	select {}
}
