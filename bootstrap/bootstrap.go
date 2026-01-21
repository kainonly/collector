// Package bootstrap 提供应用依赖的初始化函数。
//
// 包含日志、配置加载、NATS、JetStream、KV、MongoDB 等组件的创建。
package bootstrap

import (
	"context"
	"os"
	"strings"

	"github.com/kainonly/collector/v3/common"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// SetZap 初始化 zap 日志记录器。
//
// 根据 MODE 环境变量选择日志模式：
//   - MODE != "release"：开发模式，输出可读的彩色日志
//   - MODE == "release"：生产模式，输出 JSON 格式日志
func SetZap() (log *zap.Logger, err error) {
	if os.Getenv("MODE") != "release" {
		if log, err = zap.NewDevelopment(); err != nil {
			return
		}
	} else {
		if log, err = zap.NewProduction(); err != nil {
			return
		}
	}
	return
}

// LoadStaticValues 从 ./config/values.yml 加载应用配置。
//
// 配置文件必须存在且格式正确，否则返回错误。
func LoadStaticValues() (v *common.Values, err error) {
	v = new(common.Values)
	var b []byte
	if b, err = os.ReadFile("./config/values.yml"); err != nil {
		return
	}
	if err = yaml.Unmarshal(b, &v); err != nil {
		return
	}
	return
}

// UseMongo 创建 MongoDB 客户端连接。
//
// 使用配置中的 MongoUrl 建立连接。
// 调用者负责在程序退出时调用 Disconnect 关闭连接。
func UseMongo(v *common.Values) (*mongo.Client, error) {
	return mongo.Connect(
		options.Client().ApplyURI(v.MongoUrl),
	)
}

// UseNats 创建 NATS 连接。
//
// 连接配置：
//   - 支持多服务器地址（自动故障转移）
//   - 启用连接失败时重试
//   - 无限次重连尝试（MaxReconnects = -1）
//
// 调用者负责在程序退出时调用 Close 关闭连接。
func UseNats(values *common.Values) (nc *nats.Conn, err error) {
	if nc, err = nats.Connect(
		strings.Join(values.NatsHosts, ","),
		nats.Token(values.NatsToken),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
	); err != nil {
		return
	}
	return
}

// UseJetStream 从 NATS 连接创建 JetStream 上下文。
//
// JetStream 用于：
//   - 管理流（Stream）和消费者（Consumer）
//   - 发布和消费持久化消息
//   - 管理 KV 存储桶
func UseJetStream(nc *nats.Conn) (jetstream.JetStream, error) {
	return jetstream.New(nc)
}

// UseKeyValue 创建或更新命名空间 KV 存储桶。
//
// KV 存储桶用于存储流配置，收集器通过监听 KV 变更
// 实现动态订阅/取消订阅。
//
// 存储桶配置：
//   - Bucket: 使用命名空间作为存储桶名称
//   - Description: 存储桶描述信息
//   - History: 保留 3 个历史版本
//   - Compression: 启用压缩
func UseKeyValue(values *common.Values, js jetstream.JetStream) (jetstream.KeyValue, error) {
	return js.CreateOrUpdateKeyValue(context.TODO(), jetstream.KeyValueConfig{
		Bucket:      values.Namespace,
		Description: values.Description,
		History:     3,
		Compression: true,
	})
}

// UseDatabase 返回配置了写入关注的数据库句柄。
//
// 使用 majority 写入关注确保数据写入到多数副本后才返回成功，
// 提供更强的数据一致性保证。
func UseDatabase(v *common.Values, client *mongo.Client) *mongo.Database {
	option := options.Database().
		SetWriteConcern(writeconcern.Majority())
	return client.Database(v.MongoDatabase, option)
}
