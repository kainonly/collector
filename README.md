# Weplanx Collector

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/weplanx/collector/release.yml?label=release&style=flat-square)](https://github.com/weplanx/collector/actions/workflows/release.yml)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/weplanx/collector/testing.yml?label=testing&style=flat-square)](https://github.com/weplanx/collector/actions/workflows/testing.yml)
[![Release](https://img.shields.io/github/v/release/weplanx/collector.svg?style=flat-square&include_prereleases)](https://github.com/weplanx/collector/releases)
[![Coveralls github](https://img.shields.io/coveralls/github/weplanx/collector.svg?style=flat-square)](https://coveralls.io/github/weplanx/collector)
[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/weplanx/collector?style=flat-square)](https://github.com/weplanx/collector)
[![Go Report Card](https://goreportcard.com/badge/github.com/weplanx/collector?style=flat-square)](https://goreportcard.com/report/github.com/weplanx/collector)
[![GitHub license](https://img.shields.io/github/license/weplanx/collector?style=flat-square)](https://raw.githubusercontent.com/weplanx/collector/main/LICENSE)

A streamlined, professional queue-based collector tailored for MongoDB time-series data

## Pre-requisite

- A NATS JetStream cluster is required.
- A MongoDB replica set is required, with version 5.0 or higher.
- The transfer and collector must use the same NATS cluster, and the same application namespace.

## Deploy

A collector service that subscribes to stream queues and then writes to data.

The main container image is:

- ghcr.io/weplanx/collector:latest

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
        - image: ghcr.io/weplanx/collector:latest
          imagePullPolicy: Always
          name: collector
```

## License

[BSD-3-Clause License](https://github.com/weplanx/collector/blob/main/LICENSE)
