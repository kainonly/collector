# elastic-collector

Provides to collect data from the queue and write it to elasticsearch

[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/codexset/elastic-collector?style=flat-square)](https://github.com/codexset/elastic-collector)
[![Github Actions](https://img.shields.io/github/workflow/status/codexset/elastic-collector/release?style=flat-square)](https://github.com/codexset/elastic-collector/actions)
[![Image Size](https://img.shields.io/docker/image-size/kainonly/elastic-collector?style=flat-square)](https://hub.docker.com/r/kainonly/elastic-collector)
[![Docker Pulls](https://img.shields.io/docker/pulls/kainonly/elastic-collector.svg?style=flat-square)](https://hub.docker.com/r/kainonly/elastic-collector)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](https://raw.githubusercontent.com/codexset/elastic-collector/master/LICENSE)

![guide](https://cdn.kainonly.com/resource/elastic-collector.svg)

## Setup

Example using docker compose

```yaml
version: "3.8"
services: 
  collector:
    image: kainonly/elastic-collector
    restart: always
    volumes:
      - ./collector/config:/app/config
    ports:
      - 6000:6000
```

## Configuration

For configuration, please refer to `config/config.example.yml`

- **debug** `bool` Start debugging, ie `net/http/pprof`, access address is`http://localhost:6060`
- **listen** `string` Microservice listening address
- **elastic** `object` Elasticsearch configuration
    - **addresses** `array` hosts
    - **username** `string`
    - **password** `string`
    - **cloud_id** `string` cloud id
    - **api_key** `string` api key
- **mq** `object`
    - **drive** `string` Contains: `amqp`
    - **url** `string` E.g `amqp://guest:guest@localhost:5672/`

## Service

The service is based on gRPC and you can view `router/router.proto`

```proto
syntax = "proto3";
package elastic.collector;
service Router {
  rpc Get (GetParameter) returns (GetResponse) {
  }

  rpc Lists (ListsParameter) returns (ListsResponse) {
  }

  rpc All (NoParameter) returns (AllResponse) {
  }

  rpc Put (Information) returns (Response) {
  }

  rpc Delete (DeleteParameter) returns (Response) {
  }
}

message NoParameter {
}

message Response {
  uint32 error = 1;
  string msg = 2;
}

message Information {
  string identity = 1;
  string index = 2;
  string queue = 3;
}

message GetParameter {
  string identity = 1;
}

message GetResponse {
  uint32 error = 1;
  string msg = 2;
  Information data = 3;
}

message ListsParameter {
  repeated string identity = 1;
}

message ListsResponse {
  uint32 error = 1;
  string msg = 2;
  repeated Information data = 3;
}

message AllResponse {
  uint32 error = 1;
  string msg = 2;
  repeated string data = 3;
}

message DeleteParameter {
  string identity = 1;
}
```

#### rpc Get (GetParameter) returns (GetResponse) {}

Get collector configuration

- GetParameter
  - **identity** `string` collector id
- GetResponse
  - **error** `uint32` error code, `0` is normal
  - **msg** `string` error feedback
  - **data** `Information` result
    - **identity** `string` collector id
    - **index** `string` elasticsearch index
    - **queue** `string` Queue name of the message queue


```golang
client := pb.NewRouterClient(conn)
response, err := client.Get(context.Background(), &pb.GetParameter{
  Identity: "task",
})
```

#### rpc Lists (ListsParameter) returns (ListsResponse) {}

Lists collector configuration

- ListsParameter
  - **identity** `string` collector id
- ListsResponse
  - **error** `uint32` error code, `0` is normal
  - **msg** `string` error feedback
  - **data** `[]Information` result
    - **identity** `string` collector id
    - **index** `string` elasticsearch index
    - **queue** `string` Queue name of the message queue

```golang
client := pb.NewRouterClient(conn)
response, err := client.Lists(context.Background(), &pb.ListsParameter{
    Identity: []string{"task-1"},
})
```

#### rpc All (NoParameter) returns (AllResponse) {}

- NoParameter
- AllResponse
  - **error** `uint32` error code, `0` is normal
  - **msg** `string` error feedback
  - **data** `[]string` collector IDs

```golang
client := pb.NewRouterClient(conn)
response, err := client.All(context.Background(), &pb.NoParameter{})
```

#### rpc Put (Information) returns (Response) {}

- Information
  - **identity** `string` collector id
  - **index** `string` elasticsearch index
  - **queue** `string` Queue name of the message queue
- Response
  - **error** `uint32` error code, `0` is normal
  - **msg** `string` error feedback

```golang
client := pb.NewRouterClient(conn)
response, err := client.Put(context.Background(), &pb.Information{
    Identity: "task-1",
    Index:    "task-1",
    Queue:    `schedule`,
})
```

#### rpc Delete (DeleteParameter) returns (Response) {}

- DeleteParameter
  - **identity** `string` collector id
- Response
  - **error** `uint32` error code, `0` is normal
  - **msg** `string` error feedback

```golang
client := pb.NewRouterClient(conn)
response, err := client.Delete(context.Background(), &pb.DeleteParameter{
  Identity: "task-1",
})
```