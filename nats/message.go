package nats

import (
	"time"

	natsLib "github.com/nats-io/nats.go"
)

// natsMessage NATS Core 消息实现
type natsMessage struct {
	raw *natsLib.Msg
}

func (m *natsMessage) Subject() string {
	return m.raw.Subject
}

func (m *natsMessage) Data() []byte {
	return m.raw.Data
}

func (m *natsMessage) Ack() error {
	// NATS Core 不需要 Ack
	return nil
}

func (m *natsMessage) Nak() error {
	// NATS Core 不支持 Nak
	return nil
}

func (m *natsMessage) Timestamp() time.Time {
	return time.Now()
}

func (m *natsMessage) ReplyTo() string {
	return m.raw.Reply
}

// Respond 向请求方发送回复（NATS 原生支持）
func (m *natsMessage) Respond(data []byte) error {
	return m.raw.Respond(data)
}

// jetStreamMessage JetStream 消息实现
type jetStreamMessage struct {
	raw *natsLib.Msg
}

func (m *jetStreamMessage) Subject() string {
	return m.raw.Subject
}

func (m *jetStreamMessage) Data() []byte {
	return m.raw.Data
}

func (m *jetStreamMessage) Ack() error {
	return m.raw.Ack()
}

func (m *jetStreamMessage) Nak() error {
	return m.raw.Nak()
}

func (m *jetStreamMessage) Timestamp() time.Time {
	return time.Now()
}

func (m *jetStreamMessage) ReplyTo() string {
	return m.raw.Reply
}

// Respond 向请求方发送回复（NATS 原生支持）
func (m *jetStreamMessage) Respond(data []byte) error {
	return m.raw.Respond(data)
}
