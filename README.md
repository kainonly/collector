# Collector

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/kainonly/collector/release.yml?label=release&style=flat-square)](https://github.com/kainonly/collector/actions/workflows/release.yml)
[![Release](https://img.shields.io/github/v/release/kainonly/collector.svg?style=flat-square&include_prereleases)](https://github.com/kainonly/collector/releases)
[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/kainonly/collector?style=flat-square)](https://github.com/kainonly/collector)
[![Go Report Card](https://goreportcard.com/badge/github.com/kainonly/collector?style=flat-square)](https://goreportcard.com/report/github.com/kainonly/collector)
[![GitHub license](https://img.shields.io/github/license/kainonly/collector?style=flat-square)](https://raw.githubusercontent.com/kainonly/collector/main/LICENSE)

English | [简体中文](README_zh-CN.md)

A streamlined, queue-based collector tailored for MongoDB time-series data.

It consumes BSON payloads from NATS JetStream work-queue streams and writes batches into MongoDB on a fixed schedule. Streams and schedules are managed dynamically through a JetStream KeyValue bucket (namespace).

## Pre-requisite

- A NATS JetStream cluster is required.
- A MongoDB is required, with version 5.0 or higher.
- The transfer library and collector service must use the same NATS cluster and the same namespace.

## How It Works

1. A producer (via the `transfer` package) registers a key by:
   - creating/updating a JetStream stream named `${namespace}_${key}` (work-queue retention),
   - creating/updating a durable consumer named `default`,
   - storing the configuration as JSON into a KV bucket named `${namespace}` (KV key is `${key}`).
2. The collector starts and:
   - loads all keys from the KV bucket,
   - schedules a job for each key at `duration`,
   - in each job run, fetches up to `batch` messages, `InsertMany` into MongoDB, and ACKs messages on success.
3. The collector watches KV changes:
   - `PUT` => subscribe/schedule the key
   - `DELETE/PURGE` => delete the stream and remove the scheduled job
4. The collector responds to state queries on `${namespace}.states` (request/reply), returning next runs and last run time for a key.

## Configuration

The collector reads a YAML file at `./config/values.yml`.

Start from [values.example.yml](config/values.example.yml) and copy it to `config/values.yml`.

```yaml
mode: debug
namespace: alpha
description: lightweight queue-based collector
duration: 5s
batch: 1000
nats:
  hosts:
    - nats://127.0.0.1:4222
  token: your_token
database:
  url: mongodb://localhost:27017
  name: example
```

- `mode`: `debug` / `release` (logging uses zap; `MODE=release` enables production logger)
- `namespace`: KV bucket name, stream name prefix, and state query subject prefix
- `duration`: schedule interval (`time.Duration` format, e.g. `5s`, `1m`)
- `batch`: max messages fetched per run

## Quick Start (Local)

```bash
cp config/values.example.yml config/values.yml
go run .
```

## Producer Usage (transfer)

The `transfer` package is a helper to manage streams/config (via JetStream + KV) and publish BSON payloads.

```go
package main

import (
	"context"
	"time"

	"github.com/kainonly/collector/v3/transfer"
	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, _ := nats.Connect("nats://127.0.0.1:4222", nats.Token("your_token"))
	t, _ := transfer.New(ctx, "alpha", nc)

	_ = t.Add(ctx, transfer.Option{
		Key:         "metrics",
		Subs:        []string{"external.subject"},
		Collection:  "metrics",
		Description: "example stream",
	})

	_ = t.Send("metrics", bson.M{
		"ts":  time.Now(),
		"cpu": 0.42,
	})
}
```

## Deploy

The main container image is:

- `ghcr.io/kainonly/collector:latest`

The image expects the `collector` binary under `/app/collector` and reads configuration from `./config/values.yml` (relative to working directory).

Kubernetes deployment example (mount your `config/values.yml`):

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: collector
spec:
  selector:
    matchLabels:
      app: collector
  template:
    metadata:
      labels:
        app: collector
    spec:
      containers:
        - name: collector
          image: ghcr.io/kainonly/collector:latest
          imagePullPolicy: Always
          volumeMounts:
            - name: config
              mountPath: /app/config
      volumes:
        - name: config
          configMap:
            name: collector-config
```

## Project Layout

- `main.go`: entrypoint
- `bootstrap/`: wiring, configuration, and dependency setup (NATS, JetStream, KV, MongoDB, scheduler)
- `app/`: collector runtime (KV watch, scheduled fetch, Mongo writes)
- `transfer/`: producer helper library (stream/KV management + publish)
- `config/`: configuration examples

## License

[BSD-3-Clause License](https://github.com/kainonly/collector/blob/main/LICENSE)
