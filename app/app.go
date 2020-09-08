package app

import (
	"elastic-collector/app/controller"
	"elastic-collector/app/manage"
	"elastic-collector/app/mq"
	"elastic-collector/app/schema"
	"elastic-collector/app/types"
	pb "elastic-collector/router"
	"google.golang.org/grpc"
	"net"
	"net/http"
	_ "net/http/pprof"
)

type App struct {
	option *types.Config
}

func New(config types.Config) *App {
	app := new(App)
	app.option = &config
	return app
}

func (app *App) Start() (err error) {
	// Turn on debugging
	if app.option.Debug {
		go func() {
			http.ListenAndServe(":6060", nil)
		}()
	}
	// Start microservice
	listen, err := net.Listen("tcp", app.option.Listen)
	if err != nil {
		return
	}
	server := grpc.NewServer()
	dataset := schema.New()
	mqlib, err := mq.NewMessageQueue(app.option.Mq, dataset)
	if err != nil {
		return
	}
	manager, err := manage.NewElasticManager(
		app.option.Elastic,
		mqlib,
		dataset,
	)
	if err != nil {
		return
	}
	pb.RegisterRouterServer(
		server,
		controller.New(manager),
	)
	server.Serve(listen)
	return
}
