# Collector

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/kainonly/collector/release.yml?label=release&style=flat-square)](https://github.com/kainonly/collector/actions/workflows/release.yml)
[![Release](https://img.shields.io/github/v/release/kainonly/collector.svg?style=flat-square&include_prereleases)](https://github.com/kainonly/collector/releases)
[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/kainonly/collector?style=flat-square)](https://github.com/kainonly/collector)
[![Go Report Card](https://goreportcard.com/badge/github.com/kainonly/collector?style=flat-square)](https://goreportcard.com/report/github.com/kainonly/collector)
[![GitHub license](https://img.shields.io/github/license/kainonly/collector?style=flat-square)](https://raw.githubusercontent.com/kainonly/collector/main/LICENSE)

[English](README.md) | 简体中文

专为 MongoDB 时序数据设计的精简、专业的队列采集器。

## 前提条件

- 需要 NATS JetStream 集群。
- 需要 MongoDB，版本 5.0 或更高。
- 传输（transfer）和采集器（collector）必须使用相同的 NATS 集群和相同的应用命名空间。

## 部署

订阅流队列并将数据写入数据库的采集器服务。

主容器镜像为：

- ghcr.io/kainonly/collector:latest

此案例使用 Kubernetes 部署编排，副本部署（请根据需要修改）。

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

## 许可证

[BSD-3-Clause License](https://github.com/kainonly/collector/blob/main/LICENSE)
