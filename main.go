// Package main 是收集器服务的入口点。
//
// 收集器是一个队列驱动的 MongoDB 时序数据采集服务，
// 从 NATS JetStream 工作队列流中消费 BSON 数据，
// 按固定调度批量写入 MongoDB。
package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/kainonly/collector/v3/app"
	"github.com/kainonly/collector/v3/bootstrap"
	"github.com/kainonly/collector/v3/common"
)

func main() {
	// 初始化日志记录器
	// 根据 MODE 环境变量选择开发模式或生产模式
	var err error
	if common.Log, err = bootstrap.SetZap(); err != nil {
		panic(err)
	}

	// 从 config/values.yml 加载配置
	values, err := bootstrap.LoadStaticValues()
	if err != nil {
		panic(err)
	}

	// 建立 NATS 连接，支持自动重连
	nc, err := bootstrap.UseNats(values)
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	// 创建 JetStream 上下文，用于流和消费者操作
	js, err := bootstrap.UseJetStream(nc)
	if err != nil {
		panic(err)
	}

	// 获取或创建命名空间 KV 存储桶，用于存储流配置
	kv, err := bootstrap.UseKeyValue(values, js)
	if err != nil {
		panic(err)
	}

	// 建立 MongoDB 连接
	mc, err := bootstrap.UseMongo(values)
	if err != nil {
		panic(err)
	}
	defer mc.Disconnect(context.Background())

	// 获取数据库句柄，使用 majority 写入关注确保数据一致性
	db := bootstrap.UseDatabase(values, mc)

	// 创建可取消的上下文，用于优雅关闭
	ctx, cancel := context.WithCancel(context.Background())

	// 初始化应用实例
	x := app.New(values, nc, js, kv, db)

	// 注册状态查询端点，允许外部查询收集器状态
	if err = x.States(); err != nil {
		panic(err)
	}

	// 在后台启动主运行循环
	// 加载现有流配置并监听 KV 变更
	go func() {
		if err := x.Run(ctx); err != nil {
			common.Log.Error(err.Error())
		}
	}()

	// 等待中断信号 (Ctrl+C)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	// 优雅关闭：取消上下文并停止所有收集器
	// 停止前会刷新所有缓冲区中的数据
	cancel()
	x.Close()
}
