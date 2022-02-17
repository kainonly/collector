# Weplanx Collector

[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/weplanx/collector?style=flat-square)](https://github.com/weplanx/collector)
[![Go Report Card](https://goreportcard.com/badge/github.com/weplanx/collector?style=flat-square)](https://goreportcard.com/report/github.com/weplanx/collector)
[![Release](https://img.shields.io/github/v/release/weplanx/collector.svg?style=flat-square)](https://github.com/weplanx/collector)
[![GitHub license](https://img.shields.io/github/license/weplanx/collector?style=flat-square)](https://raw.githubusercontent.com/weplanx/collector/main/LICENSE)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fweplanx%2Fcollector.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fweplanx%2Fcollector?ref=badge_shield)

日志收集器，接收数据流并写入日志系统

> 版本 `*.*.*` 为 [elastic-collector](https://github.com/weplanx/log-collector/tree/elastic-collector) 已归档的分支项目
> ，请使用 `v*.*.*` 发布的版本

## 部署服务

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
      hosts: [ ]
      nkey:
    cls:
      secret_id:
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
  name: collector-deploy
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
          name: api
```

例如：在 Github Actions
中 `patch deployment collector-deploy --patch "$(sed "s/\${tag}/${{steps.meta.outputs.version}}/" < ./config/patch.yml)"`，国内可使用**Coding持续部署**或**云效流水线**等。

## License

[BSD-3-Clause License](https://github.com/weplanx/collector/blob/main/LICENSE)

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fweplanx%2Fcollector.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fweplanx%2Fcollector?ref=badge_large)