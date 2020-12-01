package bootstrap

import (
	"context"
	pb "elastic-collector/api"
	"elastic-collector/application/service/collector"
	"elastic-collector/application/service/elastic"
	"elastic-collector/application/service/queue"
	"elastic-collector/application/service/queue/drive"
	"elastic-collector/application/service/schema"
	"elastic-collector/config"
	"errors"
	"flag"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/gin-gonic/gin"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"reflect"
)

var (
	LoadConfigurationNotExists = errors.New("the configuration file does not exist")
	LoadQueueNotExists         = errors.New("please configure for queue settings")
)

// Load application configuration
// reference config.example.yml
func LoadConfiguration() (cfg *config.Config, err error) {
	if _, err = os.Stat("./config/autoload"); os.IsNotExist(err) {
		os.Mkdir("./config/autoload", os.ModeDir)
	}
	if _, err = os.Stat("./config/config.yml"); os.IsNotExist(err) {
		err = LoadConfigurationNotExists
		return
	}
	var bs []byte
	if bs, err = ioutil.ReadFile("./config/config.yml"); err != nil {
		return
	}
	if err = yaml.Unmarshal(bs, &cfg); err != nil {
		return
	}
	return
}

// Initialize the schema library configuration
func InitializeSchema() *schema.Schema {
	return schema.New("./config/autoload/")
}

// Initialize elasticsearch settings
// reference config.example.yml
// https://github.com/elastic/go-elasticsearch
func InitializeElastic(cfg *config.Config) (es *elastic.Elastic, err error) {
	es = new(elastic.Elastic)
	if es.Client, err = elasticsearch.NewClient(cfg.Elastic); err != nil {
		return
	}
	return
}

// Initialize message queue settings
// reference config.example.yml
// amqp https://github.com/streadway/amqp
func InitializeQueue(cfg *config.Config, dep drive.Dependency) (mq *queue.Queue, err error) {
	option := cfg.Queue
	if reflect.DeepEqual(option, queue.Option{}) {
		err = LoadQueueNotExists
		return
	}
	mq = new(queue.Queue)
	switch option.Drive {
	case "amqp":
		if mq.Drive, err = drive.InitializeAMQP(
			drive.AMQPOption{
				Url: option.Option["url"].(string),
			},
			&dep,
		); err != nil {
			return
		}
		break
	}
	return
}

// Initialize collector management service
func InitializeCollector(dep collector.Dependency) (*collector.Collector, error) {
	return collector.New(&dep)
}

// Set up api gateway for grpc
func ApiGateway(cfg *config.Config) (err error) {
	if cfg.Debug != "" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	ctx := context.Background()
	mux := runtime.NewServeMux()
	endpoint := *flag.String("grpc-server-endpoint", cfg.Listen, "gRPC server endpoint")
	opts := []grpc.DialOption{grpc.WithInsecure()}
	if err = pb.RegisterAPIHandlerFromEndpoint(ctx, mux, endpoint, opts); err != nil {
		return
	}
	serve := gin.New()
	serve.Any("/*any", gin.WrapF(mux.ServeHTTP))
	serve.Run(cfg.Gateway)
	return
}
