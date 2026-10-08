package nats

import (
	"fmt"
	"time"

	ce "github.com/go-meridian/codeerror"
	"github.com/go-meridian/logger"
	"github.com/go-meridian/mq"
	natsLib "github.com/nats-io/nats.go"
)

func init() {
	mq.RegisterFactory(mq.ModeNATS, func(cfg *mq.Config) (mq.MQClient, error) {
		if cfg.NATS == nil {
			return nil, fmt.Errorf("nats config is required")
		}
		return NewNATSClient(cfg.NATS)
	})
}

// natsClient NATS 客户端实现
type natsClient struct {
	conn   *natsLib.Conn
	logger *logger.Logger
}

// NewNATSClient 创建 NATS 客户端
func NewNATSClient(cfg *mq.NATSConfig) (mq.MQClient, error) {
	nc, err := natsLib.Connect(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("nats connect error: %w", err)
	}

	log := mq.GetLogger()
	log.Info("NATS connected", logger.String("url", cfg.URL))

	return &natsClient{
		conn:   nc,
		logger: log,
	}, nil
}

// Publish 异步发布消息（等待 flush 确保写入网络）
func (c *natsClient) Publish(subject string, data []byte) *ce.CodeError {
	if err := c.conn.Publish(subject, data); err != nil {
		return mq.MQPublishError.Msg("nats publish: " + err.Error())
	}
	if err := c.conn.Flush(); err != nil {
		return mq.MQPublishError.Msg("nats flush: " + err.Error())
	}
	return nil
}

// Request 同步请求-等待回复
func (c *natsClient) Request(subject string, data []byte, timeoutMs int) ([]byte, *ce.CodeError) {
	timeout := time.Duration(timeoutMs) * time.Millisecond
	msg, err := c.conn.Request(subject, data, timeout)
	if err != nil {
		return nil, mq.MQRequestError.Msg("nats request: " + err.Error())
	}
	return msg.Data, nil
}

// RequestAsync 异步请求-等待回复，立即返回结果通道，不阻塞调用方
func (c *natsClient) RequestAsync(subject string, data []byte, timeoutMs int) <-chan mq.RequestResult {
	ch := make(chan mq.RequestResult, 1)
	go func() {
		resp, ce := c.Request(subject, data, timeoutMs)
		ch <- mq.RequestResult{Data: resp, Err: ce}
		close(ch)
	}()
	return ch
}

// Subscribe 订阅主题（Core 模式）
func (c *natsClient) Subscribe(subject string, handler func(msg mq.Message)) (mq.Subscription, *ce.CodeError) {
	sub := &natsSubscription{
		conn:    c.conn,
		subject: subject,
		handler: handler,
		logger:  c.logger,
	}

	if err := sub.start(); err != nil {
		return nil, mq.MQSubscribeError.Msg("nats subscribe: " + err.Error())
	}

	return sub, nil
}

// SubscribeQueue 订阅队列（JetStream Pull 订阅模式）
func (c *natsClient) SubscribeQueue(cfg *mq.QueueConfig, handler func(msg mq.Message)) (mq.Subscription, *ce.CodeError) {
	// 确保 Stream 和 Consumer 存在
	if ce := c.EnsureQueue(cfg); ce != nil {
		return nil, ce
	}

	js, err := c.conn.JetStream()
	if err != nil {
		return nil, mq.MQStreamError.Msg("jetstream context: " + err.Error())
	}

	subject := fmt.Sprintf("%s.*", cfg.StreamSubject)
	sub, err := js.PullSubscribe(subject, cfg.ConsumerName)
	if err != nil {
		return nil, mq.MQSubscribeError.Msg("pull subscribe: " + err.Error())
	}

	pool := mq.NewWorkerPool(cfg.WorkerCount, handler)
	pool.Start()

	queueSub := &natsQueueSubscription{
		sub:        sub,
		pool:       pool,
		streamName: cfg.StreamName,
		logger:     c.logger,
	}
	queueSub.start()

	return queueSub, nil
}

// EnsureQueue 确保 JetStream Stream 和 Consumer 存在
func (c *natsClient) EnsureQueue(cfg *mq.QueueConfig) *ce.CodeError {
	js, err := c.conn.JetStream()
	if err != nil {
		return mq.MQStreamError.Msg("jetstream context: " + err.Error())
	}

	streamCfg := &natsLib.StreamConfig{
		Name:      cfg.StreamName,
		Subjects:  []string{fmt.Sprintf("%s.*", cfg.StreamSubject)},
		Storage:   natsLib.FileStorage,
		Retention: natsLib.WorkQueuePolicy,
		MaxAge:    24 * time.Hour,
	}

	_, err = js.AddStream(streamCfg)
	if err != nil && !isAlreadyExists(err) {
		return mq.MQStreamError.Msg("add stream: " + err.Error())
	}
	c.logger.Info("JetStream stream ensured", logger.String("stream", cfg.StreamName))

	consumerCfg := &natsLib.ConsumerConfig{
		Durable:       cfg.ConsumerName,
		DeliverPolicy: natsLib.DeliverAllPolicy,
		AckPolicy:     natsLib.AckExplicitPolicy,
		AckWait:       time.Duration(cfg.AckWait) * time.Second,
		MaxDeliver:    cfg.MaxDeliver,
		BackOff: []time.Duration{
			1 * time.Second,
			5 * time.Second,
			15 * time.Second,
		},
	}

	_, err = js.AddConsumer(cfg.StreamName, consumerCfg)
	if err != nil && !isAlreadyExists(err) {
		return mq.MQConsumerError.Msg("add consumer: " + err.Error())
	}
	c.logger.Info("JetStream consumer ensured", logger.String("consumer", cfg.ConsumerName))

	return nil
}

// IsConnected 检查连接状态
func (c *natsClient) IsConnected() bool {
	return c.conn != nil && c.conn.IsConnected()
}

// Close 关闭连接
func (c *natsClient) Close() {
	if c.conn != nil {
		c.conn.Close()
		c.logger.Info("NATS connection closed")
	}
}

// isAlreadyExists 检查错误是否为"已存在"
func isAlreadyExists(err error) bool {
	return err == natsLib.ErrStreamNameAlreadyInUse ||
		err == natsLib.ErrConsumerNameAlreadyInUse
}
