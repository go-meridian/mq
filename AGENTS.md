# AGENTS.md

This file provides guidance to the AI agent when working with code in this repository.

## Project

Go library (`github.com/go-meridian/mq`) providing a unified MQ client interface with NATS and Redis backends. Uses factory pattern: sub-packages register via `init()` + `RegisterFactory()`.

### 架构决策

**单仓库多包结构**，redis 和 nats 作为子包不拆分为独立模块。原因：依赖隔离已由 Go 模块系统天然保证（side-effect import 按需引入），代码总量仅 1500+ 行，管理多仓库的开销远超收益。

### 项目结构

```
mq/
├── mq.go          # 工厂注册表 + NewClient()
├── client.go      # MQClient / Subscription 接口定义
├── config.go      # Mode, Config, NATSConfig, RedisConfig, QueueConfig
├── message.go     # Message 接口
├── error.go       # 错误码定义（1020-1027）
├── pool.go        # WorkerPool（队列消费的并发模型）
├── doc.go         # 包文档 + 使用示例
├── redis/
│   ├── client.go      # redisClient 实现 MQClient（init 自注册）
│   ├── message.go     # redisMessage / redisStreamMessage / requestEnvelope
│   ├── subscriber.go  # PubSub / Stream / Queue 三种订阅实现
│   └── utils.go       # UUID 生成、JSON 序列化、TLS 配置等工具函数
└── nats/
    ├── client.go      # natsClient 实现 MQClient（init 自注册）
    ├── message.go     # natsMessage / jetStreamMessage
    └── subscriber.go  # Core NATS / JetStream PullSubscribe 订阅实现
```

### 核心接口

```go
// MQClient — 统一客户端
type MQClient interface {
    Publish(subject string, data []byte) *ce.CodeError
    Request(subject string, data []byte, timeoutMs int) ([]byte, *ce.CodeError)
    Subscribe(subject string, handler func(msg Message)) (Subscription, *ce.CodeError)
    SubscribeQueue(cfg *QueueConfig, handler func(msg Message)) (Subscription, *ce.CodeError)
    EnsureQueue(cfg *QueueConfig) *ce.CodeError
    IsConnected() bool
    Close()
}

// Message — 消息载体
type Message interface {
    Subject() string; Data() []byte; Ack() error; Nak() error
    Timestamp() time.Time; ReplyTo() string
}

// Subscription — 订阅句柄
type Subscription interface {
    Unsubscribe() error; IsActive() bool
}
```

### MQ 模式

| 模式 | 常量 | 后端特性 |
|------|------|----------|
| NATS | `ModeNATS` | Core NATS（Pub/Sub）+ JetStream（持久化、消费者组） |
| Redis | `ModeRedis` | 子模式：`RedisMQModeStream`（Streams + Consumer Group）/ `RedisMQModePubSub`（Pub/Sub + List） |

### WorkerPool

队列订阅（`SubscribeQueue`）内部使用 `WorkerPool` 进行并发消费：
- 默认 worker 数：`runtime.NumCPU() * 2`
- 带缓冲 channel（容量 workerCount*2）
- panic 恢复 + 自动 Nak
- 被 redis 和 nats 共享使用

## Error Types

所有错误返回使用 `*ce.CodeError`（`github.com/go-meridian/codeerror`），**禁止** Go 标准 `error` 接口。包级错误码定义在 `error.go`：

| 错误码 | 变量 | 含义 |
|--------|------|------|
| 1020 | `MQConnectError` | 连接失败 |
| 1021 | `MQPublishError` | 发布失败 |
| 1022 | `MQSubscribeError` | 订阅失败 |
| 1023 | `MQRequestError` | 请求失败 |
| 1024 | `MQStreamError` | Stream 操作失败 |
| 1025 | `MQConsumerError` | 消费者操作失败 |
| 1026 | `MQTimeoutError` | 超时 |
| 1027 | `MQClosedError` | 客户端已关闭 |

返回错误时使用 `.Msg()` 包装详细信息。

## Logging

本项目日志统一使用 `github.com/go-meridian/logger` 包，不直接依赖 `go.uber.org/zap`。
- 获取实例：`mq.GetLogger()` 或 `logger.L()` 返回 `*logger.Logger`
- 日志字段：`logger.String()`、`logger.Int()`、`logger.Error()`、`logger.Any()` 等
- logger 包必须在 mq 初始化前完成 `logger.Init()`
- 禁止使用 `fmt.Println` 或 `log` 包

## Naming Conventions

- 子包（`nats/`, `redis/`）import 父包使用 `"github.com/go-meridian/mq"`（包名即 `mq`）
- 第三方库使用短别名：`natsLib`（`github.com/nats-io/nats.go`）、`ce`（`github.com/go-meridian/codeerror`）
- Redis 包内部抽象 redisCmd 接口统一 `redis.Client` 和 `redis.ClusterClient`

## 已知限制

- **Message 接口无 `Respond()` 方法**：mq.Message 当前不支持 Request/Reply 的响应端。需要 Respond 的场景必须绕过抽象层直接使用原生客户端（如 `*natsLib.Msg.Respond()`）
- **Redis Pub/Sub 模式下 Ack/Nak 为 no-op**：Pub/Sub 无持久化，Ack/Nak 不产生实际效果
- **Request/Reply 仅 Redis 实现**：使用 LPush + BRPOP + UUID 应答队列模式

## Lobby 项目使用方式（下游消费者）

Lobby 通过 `replace` 指令引用本库（`replace github.com/go-meridian/mq => ../mq`），以下为标准集成模式。

### 1. Import（main.go）

```go
import (
    "github.com/go-meridian/mq"
    _ "github.com/go-meridian/mq/nats"   // side-effect，注册 NATS 工厂
    _ "github.com/go-meridian/mq/redis"  // side-effect，注册 Redis 工厂
)
```

必须至少导入一个实现包，否则 `mq.NewClient` 报 "unsupported mq mode"。

### 2. 初始化（main.go）

```go
// logger 包需先初始化（mq 内部依赖 logger.Get()）
logger.Init(cfg)

mqCfg := &mq.Config{
    Mode: mq.ModeNATS,
    NATS: &mq.NATSConfig{URL: cfg.NATS.URL},
}
mqClient, err := mq.NewClient(mqCfg)
defer mqClient.Close()
```

### 3. Handler 层封装（handler/nats/init.go）

Lobby 将 `mq.MQClient` 封装为 `mqPublisher`，通过 `subject + "." + cmd` 做路由：

```go
type mqPublisher struct {
    client  mq.MQClient
    subject string  // 例: "lobby.gate"
}

func (p *mqPublisher) Publish(cmd string, data []byte) error {
    subject := p.subject + "." + cmd  // "lobby.gate.ping"
    ce := p.client.Publish(subject, data)
    if ce != nil {
        return ce
    }
    return nil
}
```

### 4. 自注册模式

Lobby 使用 init() + 全局注册表模式：

- `RegisterCoreSubscription(subject, handler, workerCount)` — Core NATS 订阅
- `RegisterPublishStream(streamName, streamSubject)` — 发布 Stream
- `Register()` — 启动时批量激活所有已注册项

### 5. Subscribe 使用

```go
client.Subscribe(entry.subject, func(msg mq.Message) {
    // 处理消息
    data := msg.Data()
    // msg.Ack() / msg.Nak() 用于队列模式
})
```

### 6. Redis 在 Lobby 中的双重角色

Lobby 同时使用 Redis 做 MQ 消息和缓存/分布式锁，两者互不影响：
- **MQ 用途**：通过 `mq/redis/` 包，使用 Pub/Sub 或 Streams
- **缓存/锁用途**：直接使用 `go-redis/v9`，不经过 mq 抽象层
- Go 模块系统统一解析 go-redis 版本，无冲突

## 依赖管理

使用 vendor 目录，修改 `go.mod` 后必须执行：

```bash
go mod vendor
```

## 开发规范

### 代码审查检查项

- [ ] 所有 error 返回使用 `*ce.CodeError`，不用标准 `error`
- [ ] 错误使用 `.Msg()` 包装详情
- [ ] 日志使用 `logger` 包，禁止 `fmt.Println`
- [ ] 子包通过 `init()` + `mq.RegisterFactory()` 自注册
- [ ] 新增功能有对应测试
- [ ] 修改后运行 `go build ./...` 验证编译

### 禁止事项

- 禁止直接修改 `common/proto/` 中的 `.pb.go` 文件（Lobby 场景）
- 禁止使用 `fmt.Println` 调试
- 禁止 emoji 出现在代码和注释中
