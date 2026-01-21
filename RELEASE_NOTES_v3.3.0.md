# Release Notes v3.3.0

## 概述

v3.3.0 是一个架构重构版本，重点优化了内部实现，减少外部依赖，提升代码可维护性。与 v3.2.0 相比，在使用方式上保持高度兼容，但内部机制有显著改进。

## 主要变化

### 🏗️ 架构重构

- **移除 Wire 依赖注入框架**：简化依赖管理，使用直接的构造函数和参数传递，减少代码生成步骤
- **移除 gocron 调度库**：实现自定义的调度机制，减少第三方依赖，提升性能和可控性
- **引入 Collector 结构**：每个流配备独立的 `Collector` 实例，负责消息消费和批量写入，职责更清晰

### ⚡ 调度机制优化

- **v3.2.0**：使用 gocron 定时任务，周期性地从 JetStream 拉取消息
- **v3.3.0**：采用 Push-based 消费模式 + 定时刷新的混合机制
  - JetStream 主动推送消息到缓冲区
  - 定时器（`FlushInterval`）确保消息不会长时间滞留
  - 缓冲区达到 `BatchSize` 时立即刷新
  - 更高效的消息处理，减少延迟

### 📊 状态管理增强

- 新增 `State` 结构，记录收集器运行时状态
- 实现 NATS request/reply 状态查询接口（主题：`{namespace}.states`）
- Transfer SDK 的 `Get` 方法现在可以查询实时的缓冲区大小（`BufferSize`）
- 便于监控和调试，了解消息消费进度

### ⚙️ 配置调整

配置字段重命名，语义更清晰：

| v3.2.0 | v3.3.0 | 说明 |
|--------|--------|------|
| `duration` | `flush_interval` | 缓冲区定时刷新间隔 |
| `batch` | `batch_size` | 触发刷新的消息数量阈值 |
| `nats.hosts` | `nats_hosts` | 扁平化配置结构 |
| `nats.token` | `nats_token` | 扁平化配置结构 |
| `database.url` | `mongo_url` | 扁平化配置结构 |
| `database.name` | `mongo_database` | 扁平化配置结构 |

**配置示例对比：**

```yaml
# v3.2.0
mode: debug
namespace: alpha
duration: 5s
batch: 1000
nats:
  hosts:
    - nats://127.0.0.1:4222
  token: s3cr3t
database:
  url: mongodb://localhost:27017
  name: example

# v3.3.0
mode: debug
namespace: alpha
flush_interval: 5s
batch_size: 1000
nats_hosts:
  - nats://127.0.0.1:4222
nats_token: s3cr3t
mongo_url: mongodb://localhost:27017
mongo_database: example
```

### 📝 代码质量提升

- 所有核心代码添加详细的中文注释，包括函数说明、工作机制和使用示例
- 改进错误处理和日志输出，便于问题排查
- 添加 E2E 测试用例（`e2e/e2e_test.go`），增强测试覆盖
- 更新 `CLAUDE.md`，为 AI 辅助开发提供清晰的项目指引

## 兼容性说明

### ✅ 完全兼容

- **Transfer SDK API**：无变化，现有代码无需修改
  ```go
  t.Add(ctx, transfer.Option{...})  // ✅ 兼容
  t.Send("key", bsonData)           // ✅ 兼容
  t.Get(ctx, "key")                 // ✅ 兼容（现在返回实时状态）
  t.Remove(ctx, "key")              // ✅ 兼容
  ```

- **部署方式**：容器镜像、Kubernetes 配置无需变更
- **NATS/MongoDB 要求**：版本要求保持一致

### ⚠️ 需要调整

- **配置文件**：需要更新 `config/values.yml` 的字段名称（见上文对比）
- **开发流程**：不再需要运行 Wire 代码生成命令

## 升级指南

1. **更新配置文件**：
   ```bash
   # 备份现有配置
   cp config/values.yml config/values.yml.bak

   # 更新字段名称
   # duration → flush_interval
   # batch → batch_size
   # nats.* → nats_*
   # database.* → mongo_*
   ```

2. **拉取新版本**：
   ```bash
   go get github.com/kainonly/collector/v3@v3.3.0
   ```

3. **重新构建**：
   ```bash
   go build -o collector .
   ```

4. **验证运行**：
   ```bash
   go run .
   ```

## 性能改进

- **依赖项减少**：移除 `gocron/v2` 和 `goforj/wire`，二进制体积更小
- **调度效率**：Push-based 消费减少轮询开销，消息处理更及时
- **内存管理**：缓冲区预分配策略优化，减少运行时内存分配

## 技术细节

### 新增文件

- `app/collector.go`：独立的 Collector 实现
- `app/state.go`：状态管理和查询接口
- `e2e/e2e_test.go`：端到端测试用例

### 修改文件

- `app/app.go`：核心运行时逻辑重构（209行 → 约150行）
- `bootstrap/bootstrap.go`：依赖初始化简化
- `common/common.go`：移除 `Inject` 结构，简化类型定义
- `main.go`：增强启动流程和优雅关闭处理

### 移除文件

- `bootstrap/wire.go`：不再使用 Wire 框架
- `bootstrap/wire_gen.go`：移除生成文件

## 已知问题

无

## 后续计划

- 增加更多指标监控端点（Prometheus metrics）
- 支持更灵活的消息路由策略
- 提供 Web UI 管理界面

---

**完整变更日志**：https://github.com/kainonly/collector/compare/v3.2.0...v3.3.0
