module elastic-collector

go 1.15

require (
	github.com/elastic/go-elasticsearch/v8 v8.0.0-20200901131320-e21ad8e37e8d
	github.com/gin-gonic/gin v1.6.3
	github.com/golang/protobuf v1.4.3
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.2
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.0.1
	github.com/streadway/amqp v1.0.0
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	go.uber.org/fx v1.13.1
	go.uber.org/zap v1.10.0
	golang.org/x/net v0.0.0-20200822124328-c89045814202
	google.golang.org/genproto v0.0.0-20201019141844-1ed22bb0c154
	google.golang.org/grpc v1.33.1
	google.golang.org/protobuf v1.25.0
	gopkg.in/yaml.v3 v3.0.0-20200615113413-eeeca48fe776
)
