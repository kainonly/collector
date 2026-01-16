# Collector

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/kainonly/collector/release.yml?label=release&style=flat-square)](https://github.com/kainonly/collector/actions/workflows/release.yml)
[![Release](https://img.shields.io/github/v/release/kainonly/collector.svg?style=flat-square&include_prereleases)](https://github.com/kainonly/collector/releases)
[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/kainonly/collector?style=flat-square)](https://github.com/kainonly/collector)
[![Go Report Card](https://goreportcard.com/badge/github.com/kainonly/collector?style=flat-square)](https://goreportcard.com/report/github.com/kainonly/collector)
[![GitHub license](https://img.shields.io/github/license/kainonly/collector?style=flat-square)](https://raw.githubusercontent.com/kainonly/collector/main/LICENSE)

[English](README.md) | 简体中文

专为 MongoDB 时序数据设计的精简队列采集器。

它从 NATS JetStream 的 WorkQueue 流中消费 BSON 负载，并按固定周期批量写入 MongoDB。流与定时任务通过 JetStream KeyValue（命名空间）动态管理。

## 前提条件

- 需要 NATS JetStream 集群。
- 需要 MongoDB，版本 5.0 或更高。
- 传输库（transfer）与采集服务（collector）必须使用相同的 NATS 集群与相同的命名空间。

## 工作原理

1. 生产端（通过 `transfer` 包）注册一个 key：
   - 创建/更新 JetStream Stream，命名为 `${namespace}_${key}`（WorkQueue 保留策略）
   - 创建/更新 Durable Consumer，名称为 `default`
   - 将配置以 JSON 写入 KV Bucket `${namespace}`（KV 的 key 为 `${key}`）
2. Collector 启动后：
   - 从 KV Bucket 加载全部 key
   - 为每个 key 按 `duration` 创建一个定时任务
   - 每次任务运行从 Consumer 拉取最多 `batch` 条消息，写入 MongoDB（`InsertMany`），成功后 ACK
3. Collector 持续监听 KV 变化：
   - `PUT` => 自动订阅并创建/更新定时任务
   - `DELETE/PURGE` => 删除对应 Stream 并移除定时任务
4. Collector 通过 `${namespace}.states` 提供状态查询（Request/Reply），返回下一次运行时间列表与上一次运行时间。

## 配置

Collector 从 `./config/values.yml` 读取 YAML 配置。

建议从 [values.example.yml](config/values.example.yml) 开始，复制到 `config/values.yml` 并按环境修改。

```yaml
mode: debug
namespace: alpha
description: 轻量队列流收集器
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

- `mode`: `debug` / `release`（日志使用 zap；设置 `MODE=release` 会启用生产日志）
- `namespace`: KV Bucket 名称、Stream 名称前缀、状态查询 subject 前缀
- `duration`: 定时周期（`time.Duration` 格式，例如 `5s`、`1m`）
- `batch`: 单次任务最大拉取消息数

## 本地快速开始

```bash
cp config/values.example.yml config/values.yml
go run .
```

## 生产端用法（transfer）

`transfer` 包用于管理 JetStream stream/KV 配置并发布 BSON 负载。

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

## 部署

主容器镜像为：

- `ghcr.io/kainonly/collector:latest`

镜像内二进制为 `/app/collector`，并从 `./config/values.yml`（相对工作目录）读取配置。

Kubernetes 部署示例（挂载您的 `config/values.yml`）：

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

## 目录结构

- `main.go`: 入口
- `bootstrap/`: 配置与依赖初始化（NATS、JetStream、KV、MongoDB、scheduler）
- `app/`: Collector 运行时（KV watch、定时拉取、写入 MongoDB）
- `transfer/`: 生产端辅助库（stream/KV 管理 + publish）
- `config/`: 配置示例

## 许可证

[BSD-3-Clause License](https://github.com/kainonly/collector/blob/main/LICENSE)
