# Collector

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/kainonly/collector/release.yml?label=release&style=flat-square)](https://github.com/kainonly/collector/actions/workflows/release.yml)
[![Release](https://img.shields.io/github/v/release/kainonly/collector.svg?style=flat-square&include_prereleases)](https://github.com/kainonly/collector/releases)
[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/kainonly/collector?style=flat-square)](https://github.com/kainonly/collector)
[![Go Report Card](https://goreportcard.com/badge/github.com/kainonly/collector?style=flat-square)](https://goreportcard.com/report/github.com/kainonly/collector)
[![GitHub license](https://img.shields.io/github/license/kainonly/collector?style=flat-square)](https://raw.githubusercontent.com/kainonly/collector/main/LICENSE)

A streamlined, professional queue-based collector tailored for MongoDB time-series data

## Pre-requisite

- A NATS JetStream cluster is required.
- A MongoDB is required, with version 5.0 or higher.
- The transfer and collector must use the same NATS cluster, and the same application namespace.

## Deploy

A collector service that subscribes to stream queues and then writes to data.

The main container image is:

- ghcr.io/kainonly/collector:latest

The case will use Kubernetes deployment orchestration, replicate deployment (modify as needed).

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
        - image: ghcr.io/kainonly/collector:latest
          imagePullPolicy: Always
          name: collector
```

## License

[BSD-3-Clause License](https://github.com/kainonly/collector/blob/main/LICENSE)
