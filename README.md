# elastic-queue-logger

elasticsearch queue service logger

[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/kainonly/elastic-queue-logger?style=flat-square)](https://github.com/kainonly/elastic-queue-logger)
[![Travis](https://img.shields.io/travis/kainonly/elastic-queue-logger?style=flat-square)](https://www.travis-ci.org/kainonly/elastic-queue-logger)
[![Docker Pulls](https://img.shields.io/docker/pulls/kainonly/elastic-queue-logger.svg?style=flat-square)](https://hub.docker.com/r/kainonly/elastic-queue-logger)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](https://raw.githubusercontent.com/kainonly/elastic-queue-logger/master/LICENSE)

## Setup

Example using docker compose

```yaml
version: "3.7"
services: 
  logger:
    image: kainonly/elastic-queue-logger
    restart: always
    volumes:
      - ./logger/config:/app/config
      - ./logger/log:/app/log
```

## Configuration

For configuration, please refer to `config/config.example.yml`

- **debug** `bool` Start debugging, ie `net/http/pprof`, access address is`http://localhost:6060`
- **amqp** `object` AMQP uri `amqp://guest:guest@localhost:5672/`
- **elastic** `object` Elasticsearch configuration
    - **addresses** `array` hosts
    - **username** `string`
    - **password** `string`
    - **cloud_id** `string` cloud id
    - **api_key** `string` api key
- **log** `object` Log configuration
    - **storage** `bool` Turn on local logs
    - **storage_dir** `string` Local log storage directory
    
## Custom consumer

Create a configuration file in the autoload folder, which must consist of the following parameters, such as `test.yml`

```yaml
identity: mytest
queue: mytest
index: mytest
```

- **identity** `string` Consumer id
- **queue** `string` Subscription queue
- **index** `string` Elasticsearch index
