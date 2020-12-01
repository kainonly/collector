package application

import (
	pb "elastic-collector/api"
	"elastic-collector/application/common"
	"elastic-collector/application/controller"
	"elastic-collector/bootstrap"
	grpcMiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcZap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpcRecovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"net/http"
	_ "net/http/pprof"
)

func Application(dep common.Dependency) (err error) {
	cfg := dep.Config
	if cfg.Debug != "" {
		go http.ListenAndServe(cfg.Debug, nil)
	}
	var listen net.Listener
	if listen, err = net.Listen("tcp", cfg.Listen); err != nil {
		return
	}
	var logger *zap.Logger
	if logger, err = zap.NewProduction(); err != nil {
		return
	}
	defer logger.Sync()
	server := grpc.NewServer(
		grpc.StreamInterceptor(
			grpcMiddleware.ChainStreamServer(
				grpcZap.StreamServerInterceptor(logger),
				grpcRecovery.StreamServerInterceptor(),
			),
		),
		grpc.UnaryInterceptor(
			grpcMiddleware.ChainUnaryServer(
				grpcZap.UnaryServerInterceptor(logger),
				grpcRecovery.UnaryServerInterceptor(),
			),
		),
	)
	pb.RegisterAPIServer(
		server,
		controller.New(&dep),
	)
	go server.Serve(listen)
	if cfg.Gateway != "" {
		go bootstrap.ApiGateway(cfg)
	}
	return
}
