package mq

import ce "github.com/go-meridian/codeerror"

// MQClient 统一 MQ 客户端接口
type MQClient interface {
	// Publish 异步发布消息（不等待回复）
	Publish(subject string, data []byte) *ce.CodeError

	// Request 同步请求-等待回复（阻塞至收到回复或超时，类似 await）
	Request(subject string, data []byte, timeoutMs int) ([]byte, *ce.CodeError)

	// RequestAsync 异步请求-等待回复，立即返回结果通道，不阻塞调用方；
	// 需要结果时从通道读取（即 await），通道仅接收一次结果后关闭
	RequestAsync(subject string, data []byte, timeoutMs int) <-chan RequestResult

	// Subscribe 订阅主题（Core 模式，收到消息直接回调）
	Subscribe(subject string, handler func(msg Message)) (Subscription, *ce.CodeError)

	// SubscribeQueue 订阅队列（支持消费者组负载均衡，消息需要 Ack）
	// NATS: JetStream PullSubscribe
	// Redis: XREADGROUP Consumer Group 或 List + BRPOP
	SubscribeQueue(cfg *QueueConfig, handler func(msg Message)) (Subscription, *ce.CodeError)

	// EnsureQueue 确保队列/Stream 存在
	EnsureQueue(cfg *QueueConfig) *ce.CodeError

	// IsConnected 检查连接状态
	IsConnected() bool

	// Close 关闭连接
	Close()
}

// RequestResult 异步请求（RequestAsync）的结果
type RequestResult struct {
	// Data 回复数据（Err 非 nil 时为空）
	Data []byte

	// Err 请求失败原因（超时、无响应方、连接断开等）
	Err *ce.CodeError
}

// Subscription 订阅句柄
type Subscription interface {
	// Unsubscribe 取消订阅
	Unsubscribe() error

	// IsActive 是否活跃
	IsActive() bool
}
