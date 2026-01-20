# Collector

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/kainonly/collector/release.yml?label=release&style=flat-square)](https://github.com/kainonly/collector/actions/workflows/release.yml)
[![Release](https://img.shields.io/github/v/release/kainonly/collector.svg?style=flat-square&include_prereleases)](https://github.com/kainonly/collector/releases)
[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/kainonly/collector?style=flat-square)](https://github.com/kainonly/collector)
[![Go Report Card](https://goreportcard.com/badge/github.com/kainonly/collector?style=flat-square)](https://goreportcard.com/report/github.com/kainonly/collector)
[![GitHub license](https://img.shields.io/github/license/kainonly/collector?style=flat-square)](https://raw.githubusercontent.com/kainonly/collector/main/LICENSE)

[English](README.md) | 简体中文

轻量级时序数据采集服务。从 NATS JetStream 工作队列流中消费 BSON 数据，批量写入 MongoDB。

## 概览

![架构图](docs/plan.png)

## 特性

- 基于推送的 NATS JetStream 消息消费
- 可配置缓冲区的 MongoDB 批量写入
- 通过 JetStream KeyValue 动态管理流
- KV PUT 自动订阅，KV DELETE 自动取消订阅
- 优雅关闭，确保最后一次缓冲刷新
- 云原生设计：单收集器支持多流，水平扩展

## 前提条件

- NATS JetStream 集群
- MongoDB 5.0+ 并使用[时序集合](https://www.mongodb.com/docs/manual/core/timeseries-collections/)

> **推荐**: 使用 MongoDB 时序集合可获得最佳的存储效率和时序数据查询性能。请在发送数据前使用 `timeseries` 选项创建集合。

## 配置

创建 `config/values.yml`：

```yaml
mode: debug
namespace: alpha
batch_size: 1000
flush_interval: 5s
nats_hosts:
  - nats://127.0.0.1:4222
nats_token: your-token
mongo_url: mongodb://localhost:27017
mongo_database: example
```

| 字段 | 说明 |
|------|------|
| `mode` | `debug` 或 `release` |
| `namespace` | 应用命名空间，用于流/KV 命名 |
| `batch_size` | 达到此数量时刷新缓冲区 |
| `flush_interval` | 按此间隔刷新缓冲区 |
| `nats_hosts` | NATS 服务器地址 |
| `nats_token` | NATS 认证令牌 |
| `mongo_url` | MongoDB 连接 URL |
| `mongo_database` | MongoDB 数据库名 |

## 数据流

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   应用服务   │     │   应用服务   │     │   应用服务   │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │ 发送 BSON
                           ▼
                  ┌─────────────────┐
                  │  Transfer SDK   │
                  │   发布 BSON     │
                  └────────┬────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                     NATS JetStream                          │
│  Stream: {namespace}_{key}                                  │
│  Subject: {namespace}.{key}                                 │
│  Consumer: default (WorkQueue)                              │
│  KV Bucket: {namespace}                                     │
└───────────────────────────┬─────────────────────────────────┘
                            │ Consume()
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Collector Pod                            │
│                                                             │
│   消息 ──► 缓冲区 ──► 刷新 ──► InsertMany                    │
│              │                                              │
│      (batch_size 或 flush_interval)                         │
└───────────────────────────┬─────────────────────────────────┘
                            │ 成功: ACK / 失败: NAK
                            ▼
                  ┌─────────────────────────┐
                  │        MongoDB          │
                  └─────────────────────────┘
```

## Transfer SDK

用于管理流和发布 BSON 数据的客户端 SDK。

```go
import (
    "context"
    "time"

    "github.com/kainonly/collector/v3/transfer"
    "github.com/nats-io/nats.go"
    "go.mongodb.org/mongo-driver/v2/bson"
)

func main() {
    ctx := context.Background()
    nc, _ := nats.Connect("nats://127.0.0.1:4222", nats.Token("your-token"))

    // 创建客户端
    t, _ := transfer.New(ctx, "alpha", nc)

    // 注册流
    t.Add(ctx, transfer.Option{
        Key:         "metrics",
        Collection:  "metrics",
        Description: "指标流",
    })

    // 发布 BSON 数据
    t.Send("metrics", bson.M{
        "ts":  time.Now(),
        "cpu": 0.42,
    })

    // 查询收集器状态
    option, _ := t.Get(ctx, "metrics")
    fmt.Println(option.BufferSize)

    // 移除流
    t.Remove(ctx, "metrics")
}
```

## 快速开始

```bash
cp config/values.example.yml config/values.yml
go run .
```

## 部署

容器镜像：`ghcr.io/kainonly/collector:latest`

Kubernetes 部署示例：

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

## 项目结构

- `main.go` - 入口
- `bootstrap/` - 配置与依赖初始化
- `app/` - 收集器运行时（KV 监听、缓冲区、MongoDB 写入）
- `transfer/` - 生产者 SDK（流/KV 管理 + 发布）
- `common/` - 共享类型和日志
- `config/` - 配置示例

## 许可证

[BSD-3-Clause License](LICENSE)
