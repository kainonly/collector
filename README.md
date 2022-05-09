# Weplanx Collector

[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/weplanx/collector?style=flat-square)](https://github.com/weplanx/collector)
[![Go Report Card](https://goreportcard.com/badge/github.com/weplanx/collector?style=flat-square)](https://goreportcard.com/report/github.com/weplanx/collector)
[![Release](https://img.shields.io/github/v/release/weplanx/collector.svg?style=flat-square)](https://github.com/weplanx/collector)
[![GitHub license](https://img.shields.io/github/license/weplanx/collector?style=flat-square)](https://raw.githubusercontent.com/weplanx/collector/main/LICENSE)

日志采集服务在 NATS JetStream 基础上订阅匹配与 [Transfer](https://github.com/weplanx/transfer) 传输客户端相同的命名空间，自动进行配置调度、 日志系统写入

> 版本 `*.*.*` 为 [elastic-collector](https://github.com/weplanx/collector/tree/elastic-collector) 已归档的分支项目
> ，请使用 `v*.*.*` 发布的版本（预发布用于构建测试）

技术文档：[语雀](https://www.yuque.com/kainonly/weplanx/collector)

## License

[BSD-3-Clause License](https://github.com/weplanx/collector/blob/main/LICENSE)