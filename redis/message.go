package redis

import (
	"time"

	"github.com/go-meridian/mq"
)

// redisMessage Redis 消息实现
type redisMessage struct {
	subject   string
	data      []byte
	replyTo   string
	timestamp time.Time
}

func (m *redisMessage) Subject() string {
	return m.subject
}

func (m *redisMessage) Data() []byte {
	return m.data
}

func (m *redisMessage) Ack() error {
	// Pub/Sub 模式不需要 Ack
	// Streams 模式在 subscriber 中单独处理
	return nil
}

func (m *redisMessage) Nak() error {
	// Pub/Sub 模式不支持 Nak
	return nil
}

func (m *redisMessage) Timestamp() time.Time {
	return m.timestamp
}

func (m *redisMessage) ReplyTo() string {
	return m.replyTo
}

// Respond Redis 模式暂不支持 Request/Reply 响应
func (m *redisMessage) Respond(data []byte) error {
	return mq.MQRequestError.Msg("redis respond: not supported")
}

// redisStreamMessage Redis Streams 消息实现（支持 Ack）
type redisStreamMessage struct {
	subject   string
	streamKey string
	group     string
	id        string
	data      []byte
	timestamp time.Time
	acked     bool
	ackFn     func(id string) error
}

func (m *redisStreamMessage) Subject() string {
	return m.subject
}

func (m *redisStreamMessage) Data() []byte {
	return m.data
}

func (m *redisStreamMessage) Ack() error {
	if m.acked {
		return nil
	}
	if m.ackFn != nil {
		m.acked = true
		return m.ackFn(m.id)
	}
	return nil
}

func (m *redisStreamMessage) Nak() error {
	// Redis Streams 不原生支持 Nak，消息会超时后重新投递
	return nil
}

func (m *redisStreamMessage) Timestamp() time.Time {
	return m.timestamp
}

func (m *redisStreamMessage) ReplyTo() string {
	return ""
}

// Respond Redis 模式暂不支持 Request/Reply 响应
func (m *redisStreamMessage) Respond(data []byte) error {
	return mq.MQRequestError.Msg("redis respond: not supported")
}

// requestEnvelope 请求消息封装
type requestEnvelope struct {
	Data    []byte `json:"data"`
	ReplyTo string `json:"replyTo"`
}
