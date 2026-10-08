package mq

import "time"

// Message 统一消息接口，屏蔽 NATS/Redis 底层差异
type Message interface {
	// Subject 消息主题（NATS: subject, Redis: stream key / channel）
	Subject() string

	// Data 消息原始数据
	Data() []byte

	// Ack 确认消息处理成功
	Ack() error

	// Nak 拒绝消息（触发重投递）
	Nak() error

	// Timestamp 消息时间戳
	Timestamp() time.Time

	// ReplyTo 回复主题（Request/Reply 模式使用）
	ReplyTo() string

	// Respond 向请求方回复结果（Request/Reply 模式，服务端在 handler 中调用，
	// 调用方通过 Request 阻塞等待该回复）
	Respond(data []byte) error
}
