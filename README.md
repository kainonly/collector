# Weplanx Collector

[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/weplanx/collector?style=flat-square)](https://github.com/weplanx/collector)
[![Go Report Card](https://goreportcard.com/badge/github.com/weplanx/collector?style=flat-square)](https://goreportcard.com/report/github.com/weplanx/collector)
[![Release](https://img.shields.io/github/v/release/weplanx/collector.svg?style=flat-square)](https://github.com/weplanx/collector)
[![GitHub license](https://img.shields.io/github/license/weplanx/collector?style=flat-square)](https://raw.githubusercontent.com/weplanx/collector/main/LICENSE)

日志采集器在 NATS JetStream 基础上订阅匹配与 [Transfer](https://github.com/weplanx/transfer) 服务相同的命名空间，自动进行配置调度、 日志系统写入

> 版本 `*.*.*` 为 [elastic-collector](https://github.com/weplanx/log-collector/tree/elastic-collector) 已归档的分支项目
> ，请使用 `v*.*.*` 发布的版本（预发布用于构建测试）

## 部署服务

消费数据可选择腾讯云 CLS 日志系统写入，通过 CLS 可进行更多自定义，例如：投递至智能分层的 COS 对象存储中替代永久存储等。也支持写入到自定的 influxDB 数据库中做定制处理。

![Collector](./topology.png)

镜像源主要有：

- ghcr.io/weplanx/collector:latest
- ccr.ccs.tencentyun.com/weplanx/collector:latest（国内）

案例将使用 Kubernetes 部署编排，复制部署内容（需要根据情况做修改）：

1. 设置配置

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: collector.cfg
data:
  config.yml: |
    namespace: <命名空间>
    nats:
      hosts:
        - "nats://a.nats:4222"
        - "nats://b.nats:4222"
        - "nats://c.nats:4222"
      nkey: "<nkey>"
    log_system:
      type: "cls"
      option:
        secret_id: <建议创建CLS子用户，https://cloud.tencent.com/document/product/598/13674>
        secret_key: 
        endpoint: ap-guangzhou.cls.tencentcs.com
        topic_id: <日志主题ID>
```

2. 部署

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    app: collector
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
        - image: ccr.ccs.tencentyun.com/weplanx/collector:latest
          imagePullPolicy: Always
          name: collector
          volumeMounts:
            - name: config
              mountPath: "/app/config"
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: collector.cfg
            items:
              - key: "config.yml"
                path: "config.yml"
```

## 滚动更新

复制模板内容，并需要自行定制触发条件，原理是每次patch将模板中 `${tag}` 替换为版本执行

```yml
spec:
  template:
    spec:
      containers:
        - image: ccr.ccs.tencentyun.com/weplanx/collector:${tag}
          name: collector
```

例如：在 Github Actions 中

```shell
patch deployment collector --patch "$(sed "s/\${tag}/${{steps.meta.outputs.version}}/" < ./config/patch.yml)"
```

国内可使用 **Coding持续部署** 或 **云效流水线** 等

## License

[BSD-3-Clause License](https://github.com/weplanx/collector/blob/main/LICENSE)

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fweplanx%2Fcollector.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fweplanx%2Fcollector?ref=badge_large)