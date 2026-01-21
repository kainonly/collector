// Package common 提供跨包共享的类型和全局变量。
package common

import (
	"time"

	"go.uber.org/zap"
)

// Log 是全局日志记录器实例。
// 在 main 函数中通过 bootstrap.SetZap() 初始化。
var Log *zap.Logger

// Values 定义从 config/values.yml 加载的应用配置。
type Values struct {
	// Mode 运行模式：debug 或 release
	// debug 模式使用开发日志格式，release 模式使用生产日志格式
	Mode string `yaml:"mode"`

	// Namespace 应用命名空间，用于：
	// - JetStream 流名称前缀：{namespace}_{key}
	// - 订阅主题前缀：{namespace}.{key}
	// - KV 存储桶名称：{namespace}
	// 注意：不能包含连字符 '-'
	Namespace string `yaml:"namespace"`

	// Description KV 存储桶的描述信息
	Description string `yaml:"description"`

	// BatchSize 缓冲区达到此数量时触发写入 MongoDB
	BatchSize int `yaml:"batch_size"`

	// FlushInterval 定时刷新间隔，即使缓冲区未满也会写入
	FlushInterval time.Duration `yaml:"flush_interval"`

	// NatsHosts NATS 服务器地址列表
	NatsHosts []string `yaml:"nats_hosts"`

	// NatsToken NATS 认证令牌
	NatsToken string `yaml:"nats_token"`

	// MongoUrl MongoDB 连接 URL
	MongoUrl string `yaml:"mongo_url"`

	// MongoDatabase MongoDB 数据库名称
	MongoDatabase string `yaml:"mongo_database"`
}
