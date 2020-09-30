package app

import (
	"elastic-collector/app/controller"
	"elastic-collector/app/manage"
	"elastic-collector/app/mq"
	"elastic-collector/app/schema"
	"elastic-collector/app/types"
	pb "elastic-collector/router"
	"github.com/elastic/go-elasticsearch/v8"
	"google.golang.org/grpc"
	"net"
	"net/http"
	_ "net/http/pprof"
)

func Application(option *types.Config) (err error) {
	// Turn on debugging
	if option.Debug != "" {
		go func() {
			http.ListenAndServe(option.Debug, nil)
		}()
	}
	// Start microservice
	listen, err := net.Listen("tcp", option.Listen)
	if err != nil {
		return
	}
	server := grpc.NewServer()
	elastic, err := elasticsearch.NewClient(option.Elastic)
	if err != nil {
		return
	}
	dataset := schema.New()
	mqclient, err := mq.NewMessageQueue(option.Mq, elastic, dataset)
	if err != nil {
		return
	}
	manager, err := manage.NewElasticManager(
		elastic,
		mqclient,
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
