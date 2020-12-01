# elastic-collector

Provides to collect data from the queue and write it to elasticsearch

[![Github Actions](https://img.shields.io/github/workflow/status/kain-lab/elastic-collector/release?style=flat-square)](https://github.com/kain-lab/elastic-collector/actions)
[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/kain-lab/elastic-collector?style=flat-square)](https://github.com/kain-lab/elastic-collector)
[![Image Size](https://img.shields.io/docker/image-size/kainonly/elastic-collector?style=flat-square)](https://hub.docker.com/r/kainonly/elastic-collector)
[![Docker Pulls](https://img.shields.io/docker/pulls/kainonly/elastic-collector.svg?style=flat-square)](https://hub.docker.com/r/kainonly/elastic-collector)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](https://raw.githubusercontent.com/kainonly/elastic-collector/master/LICENSE)

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
      - 8080:8080
```

## Configuration

For configuration, please refer to `config/config.example.yml`

- **debug** `string` Start debugging, ie `net/http/pprof`, access address is`http://localhost:6060`
- **listen** `string` grpc server listening address
- **gateway** `string` API gateway server listening address
- **elastic** `object` Elasticsearch configuration
    - **addresses** `array` hosts
    - **username** `string`
    - **password** `string`
    - **cloud_id** `string` cloud id
    - **api_key** `string` api key
- **queue** `object`
    - **drive** `string` Contains: `amqp`
    - **option** `object` (amqp) 
        - **url** `string` E.g `amqp://guest:guest@localhost:5672/`

## Service

The service is based on gRPC to view `api/api.proto`

```proto
syntax = "proto3";
package elastic.collector;
option go_package = "elastic-collector/gen/go/elastic/collector";
import "google/protobuf/empty.proto";
import "google/api/annotations.proto";

service API {
  rpc Get (ID) returns (Data) {
    option (google.api.http) = {
      get: "/collector",
    };
  }
  rpc Lists (IDs) returns (DataLists) {
    option (google.api.http) = {
      post: "/collectors",
      body: "*"
    };
  }
  rpc All (google.protobuf.Empty) returns (IDs) {
    option (google.api.http) = {
      get: "/collectors",
    };
  }
  rpc Put (Data) returns (google.protobuf.Empty) {
    option (google.api.http) = {
      put: "/collector",
      body: "*",
    };
  }
  rpc Delete (ID) returns (google.protobuf.Empty) {
    option (google.api.http) = {
      delete: "/collector",
    };
  }
}

message Data {
  string id = 1;
  string index = 2;
  string queue = 3;
}

message ID {
  string id = 1;
}

message IDs {
  repeated string ids = 1;
}

message DataLists {
  repeated Data data = 1;
}
```

## Get (ID) returns (Data)

Get collector configuration

### RPC

- **ID**
  - **id** `string` collector id
- **Data**
  - **id** `string` collector id
  - **index** `string` elasticsearch index
  - **queue** `string` Queue name of the message queue


```golang
client := pb.NewRouterClient(conn)
response, err := client.Get(context.Background(), &pb.ID{
  Id: "debug",
})
```

### API Gateway

- **GET** `/collector`

```http
GET /collector?id=debug HTTP/1.1
Host: localhost:8080
```

## Lists (IDs) returns (DataLists)

Lists collector configuration

### RPC

- **IDs**
  - **ids** `[]string` collector id
- **DataLists**
  - **data** `[]Data` result
    - **identity** `string` collector id
    - **index** `string` elasticsearch index
    - **queue** `string` Queue name of the message queue

```golang
client := pb.NewRouterClient(conn)
response, err := client.Lists(context.Background(), &pb.IDs{
  Ids: []string{"debug"},
})
```

### API Gateway

- **POST** `/collectors`

```http
POST /collectors HTTP/1.1
Host: localhost:8080
Content-Type: application/json

{
    "ids":["debug"]
}
```

## All (google.protobuf.Empty) returns (IDs)

Get all collector configuration identifiers

### RPC

- **IDs**
  - **ids** `[]string` collector id

```golang
client := pb.NewRouterClient(conn)
response, err := client.All(context.Background(), &empty.Empty{})
```

### API Gateway

- **GET** `/collectors`

```http
GET /collectors HTTP/1.1
Host: localhost:8080
```

## Put (Data) returns (google.protobuf.Empty)

Put collector configuration

### RPC

- **Data**
  - **id** `string` collector id
  - **index** `string` elasticsearch index
  - **queue** `string` Queue name of the message queue

```golang
client := pb.NewRouterClient(conn)
response, err := client.Put(context.Background(), &pb.Data{
  Id:    "debug",
  Index: "debug-logs-alpha",
  Queue: "debug",
})
```

### API Gateway

- **PUT** `/collector`

```http
PUT /collector HTTP/1.1
Host: localhost:8080
Content-Type: application/json

{
    "id": "debug",
    "index": "debug-logs-alpha",
    "queue": "debug"
}
```

## Delete (ID) returns (google.protobuf.Empty) {}

Remove collector configuration

### RPC

- **ID**
  - **id** `string` collector id

```golang
client := pb.NewRouterClient(conn)
response, err := client.Delete(context.Background(), &pb.ID{
  Id: "debug",
})
```

### API Gateway

- **DELETE** `/collector`

```http
DELETE /collector?id=debug HTTP/1.1
Host: localhost:8080
```