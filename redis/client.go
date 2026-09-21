package redis

import (
	"context"
	"fmt"
	"time"

	ce "github.com/go-meridian/codeerror"
	"github.com/go-meridian/logger"
	"github.com/go-meridian/mq"
	"github.com/redis/go-redis/v9"
)

func init() {
	mq.RegisterFactory(mq.ModeRedis, func(cfg *mq.Config) (mq.MQClient, error) {
		if cfg.Redis == nil {
			return nil, fmt.Errorf("redis config is required")
		}
		return NewRedisClient(cfg.Redis)
	})
}

// redisCmd 统一 Redis 命令接口，兼容单机和集群
type redisCmd interface {
	Ping(ctx context.Context) *redis.StatusCmd
	XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
	XRead(ctx context.Context, a *redis.XReadArgs) *redis.XStreamSliceCmd
	XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd
	XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd
	XLen(ctx context.Context, stream string) *redis.IntCmd
	XInfoStream(ctx context.Context, key string) *redis.XInfoStreamCmd
	XPending(ctx context.Context, stream, group string) *redis.XPendingCmd
	XClaim(ctx context.Context, a *redis.XClaimArgs) *redis.XMessageSliceCmd
	XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd
	Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
	LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	BRPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd
	Close() error
}

// batchMsg 批量发送消息
type batchMsg struct {
	subject   string
	streamKey string
	data      []byte
}

// redisClient Redis 客户端实现
type redisClient struct {
	rdb       redisCmd
	mqMode    mq.RedisMQMode
	logger    *logger.Logger
	ctx       context.Context
	cancel    context.CancelFunc
	maxLen    int64
	batchSize int
	sendChan  chan *batchMsg
}

// NewRedisClient 创建 Redis 客户端
func NewRedisClient(cfg *mq.RedisConfig) (mq.MQClient, error) {
	var rdb redisCmd

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	poolSize := orDefault(cfg.PoolSize, 100)
	minIdle := orDefault(cfg.MinIdleConns, 10)
	maxRetries := orDefault(cfg.MaxRetries, 1)
	dialTimeout := time.Duration(orDefault(cfg.DialTimeout, 5000)) * time.Millisecond
	readTimeout := time.Duration(orDefault(cfg.ReadTimeout, 3000)) * time.Millisecond
	writeTimeout := time.Duration(orDefault(cfg.WriteTimeout, 3000)) * time.Millisecond
	tlsCfg := buildTLSConfig(cfg.IsTLS)

	if cfg.IsCluster {
		rdb = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        []string{addr},
			Password:     cfg.Password,
			PoolSize:     poolSize,
			MinIdleConns: minIdle,
			MaxRetries:   maxRetries,
			DialTimeout:  dialTimeout,
			ReadTimeout:  readTimeout,
			WriteTimeout: writeTimeout,
			TLSConfig:    tlsCfg,
		})
	} else {
		rdb = redis.NewClient(&redis.Options{
			Addr:         addr,
			Password:     cfg.Password,
			DB:           cfg.DB,
			PoolSize:     poolSize,
			MinIdleConns: minIdle,
			MaxRetries:   maxRetries,
			DialTimeout:  dialTimeout,
			ReadTimeout:  readTimeout,
			WriteTimeout: writeTimeout,
			TLSConfig:    tlsCfg,
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := rdb.Ping(ctx).Err(); err != nil {
		cancel()
		return nil, fmt.Errorf("redis connect error: %w", err)
	}

	log := mq.GetLogger()
	log.Info("Redis connected",
		logger.String("addr", addr),
		logger.Int("db", cfg.DB),
		logger.String("mqMode", string(cfg.MQMode)),
		logger.Int("poolSize", poolSize),
		logger.Int("minIdleConns", minIdle),
		logger.Bool("isCluster", cfg.IsCluster),
		logger.Bool("isTLS", cfg.IsTLS),
	)

	client := &redisClient{
		rdb:       rdb,
		mqMode:    cfg.MQMode,
		logger:    log,
		ctx:       ctx,
		cancel:    cancel,
		maxLen:    cfg.StreamMaxLen,
		batchSize: cfg.BatchSize,
	}

	// 批量发送模式
	if cfg.BatchSize > 1 {
		client.sendChan = make(chan *batchMsg, 20480)
		client.startBatchSender()
	}

	return client, nil
}

// Publish 发布消息
func (c *redisClient) Publish(subject string, data []byte) *ce.CodeError {
	switch c.mqMode {
	case mq.RedisMQModePubSub:
		return c.publishPubSub(subject, data)
	case mq.RedisMQModeStream:
		return c.publishStream(subject, data)
	default:
		return c.publishStream(subject, data)
	}
}

// publishPubSub 使用 Pub/Sub 发布（无持久化）
func (c *redisClient) publishPubSub(subject string, data []byte) *ce.CodeError {
	channel := "mq:pubsub:" + subject
	if err := c.rdb.Publish(c.ctx, channel, data).Err(); err != nil {
		return mq.MQPublishError.Msg("redis publish: " + err.Error())
	}
	return nil
}

// publishStream 使用 Streams 发布（有持久化）
func (c *redisClient) publishStream(subject string, data []byte) *ce.CodeError {
	// 批量发送模式
	if c.sendChan != nil {
		c.sendChan <- &batchMsg{
			subject:   subject,
			streamKey: "mq:stream:" + subject,
			data:      data,
		}
		return nil
	}

	// 同步单条发送
	streamKey := "mq:stream:" + subject
	args := &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: c.maxLen,
		Approx: c.maxLen > 0,
		Values: map[string]interface{}{
			"data": data,
		},
	}
	if err := c.rdb.XAdd(c.ctx, args).Err(); err != nil {
		return mq.MQPublishError.Msg("redis xadd: " + err.Error())
	}
	return nil
}

// startBatchSender 启动批量发送 goroutine（参考 BalloonService Producer）
func (c *redisClient) startBatchSender() {
	go func() {
		// 按 streamKey 分组的 batch
		batches := make(map[string]map[string]interface{})
		indexes := make(map[string]int)

		for {
			select {
			case <-c.ctx.Done():
				c.flushAllBatches(batches)
				return
			case msg := <-c.sendChan:
				batch := batches[msg.streamKey]
				if batch == nil {
					batch = make(map[string]interface{}, c.batchSize)
					batches[msg.streamKey] = batch
				}
				idx := indexes[msg.streamKey]
				cid := fmt.Sprintf("%s-%d", msg.subject, idx)
				batch[cid] = msg.data
				indexes[msg.streamKey] = idx + 1

				if len(batch) >= c.batchSize {
					c.flushBatch(msg.streamKey, batch)
					batches[msg.streamKey] = make(map[string]interface{}, c.batchSize)
					indexes[msg.streamKey] = 0
				}
			default:
				if len(batches) > 0 {
					c.flushAllBatches(batches)
					batches = make(map[string]map[string]interface{})
					indexes = make(map[string]int)
				} else {
					select {
					case <-c.ctx.Done():
						return
					case msg := <-c.sendChan:
						batch := batches[msg.streamKey]
						if batch == nil {
							batch = make(map[string]interface{}, c.batchSize)
							batches[msg.streamKey] = batch
						}
						idx := indexes[msg.streamKey]
						cid := fmt.Sprintf("%s-%d", msg.subject, idx)
						batch[cid] = msg.data
						indexes[msg.streamKey] = idx + 1
					}
				}
			}
		}
	}()
}

// flushBatch 刷新单个 stream 的 batch
func (c *redisClient) flushBatch(streamKey string, batch map[string]interface{}) {
	args := &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: c.maxLen,
		Approx: c.maxLen > 0,
		Values: batch,
	}
	if err := c.rdb.XAdd(c.ctx, args).Err(); err != nil {
		c.logger.Error("batch xadd failed",
			logger.String("stream", streamKey),
			logger.Error(err),
		)
	}
}

// flushAllBatches 刷新所有 stream 的 batch
func (c *redisClient) flushAllBatches(batches map[string]map[string]interface{}) {
	for streamKey, batch := range batches {
		if len(batch) > 0 {
			c.flushBatch(streamKey, batch)
		}
	}
}

// Request 同步请求-等待回复（使用 List + BRPOP）
func (c *redisClient) Request(subject string, data []byte, timeoutMs int) ([]byte, *ce.CodeError) {
	replyID := generateID()
	replyKey := "mq:reply:" + replyID

	envelope := &requestEnvelope{
		Data:    data,
		ReplyTo: replyKey,
	}
	payload, err := jsonMarshal(envelope)
	if err != nil {
		return nil, mq.MQRequestError.Msg("marshal envelope: " + err.Error())
	}

	reqKey := "mq:req:" + subject
	if err := c.rdb.LPush(c.ctx, reqKey, payload).Err(); err != nil {
		return nil, mq.MQRequestError.Msg("redis lpush: " + err.Error())
	}

	timeout := fmt.Sprintf("%dms", timeoutMs)
	result, err := c.rdb.BRPop(c.ctx, parseDuration(timeout), replyKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, mq.MQTimeoutError.Msg("redis brpop timeout")
		}
		return nil, mq.MQRequestError.Msg("redis brpop: " + err.Error())
	}

	return []byte(result[1]), nil
}

// Subscribe 订阅主题
func (c *redisClient) Subscribe(subject string, handler func(msg mq.Message)) (mq.Subscription, *ce.CodeError) {
	switch c.mqMode {
	case mq.RedisMQModePubSub:
		return c.subscribePubSub(subject, handler)
	case mq.RedisMQModeStream:
		return c.subscribeStream(subject, handler)
	default:
		return c.subscribeStream(subject, handler)
	}
}

// subscribePubSub 使用 Pub/Sub 订阅
func (c *redisClient) subscribePubSub(subject string, handler func(msg mq.Message)) (mq.Subscription, *ce.CodeError) {
	channel := "mq:pubsub:" + subject
	pubsub := c.rdb.Subscribe(c.ctx, channel)

	if _, err := pubsub.Receive(c.ctx); err != nil {
		return nil, mq.MQSubscribeError.Msg("redis subscribe: " + err.Error())
	}

	sub := &redisPubSubSubscription{
		pubsub:  pubsub,
		subject: subject,
		handler: handler,
		logger:  c.logger,
		ctx:     c.ctx,
	}
	sub.start()

	return sub, nil
}

// subscribeStream 使用 Streams 订阅
func (c *redisClient) subscribeStream(subject string, handler func(msg mq.Message)) (mq.Subscription, *ce.CodeError) {
	streamKey := "mq:stream:" + subject

	sub := &redisStreamSubscription{
		rdb:       c.rdb,
		streamKey: streamKey,
		subject:   subject,
		handler:   handler,
		logger:    c.logger,
		ctx:       c.ctx,
	}
	sub.start()

	return sub, nil
}

// SubscribeQueue 订阅队列（支持消费者组负载均衡）
func (c *redisClient) SubscribeQueue(cfg *mq.QueueConfig, handler func(msg mq.Message)) (mq.Subscription, *ce.CodeError) {
	if ce := c.EnsureQueue(cfg); ce != nil {
		return nil, ce
	}

	streamKey := "mq:stream:" + cfg.StreamName
	group := cfg.ConsumerName
	consumer := fmt.Sprintf("%s-%s", group, generateID()[:8])

	pool := mq.NewWorkerPool(cfg.WorkerCount, handler)
	pool.Start()

	sub := &redisQueueSubscription{
		rdb:       c.rdb,
		streamKey: streamKey,
		group:     group,
		consumer:  consumer,
		pool:      pool,
		logger:    c.logger,
		ctx:       c.ctx,
		readCount: orDefault(cfg.ReadCount, 100),
		lastRead:  "0",
	}
	sub.start()

	return sub, nil
}

// EnsureQueue 确保 Redis Stream 和 Consumer Group 存在
func (c *redisClient) EnsureQueue(cfg *mq.QueueConfig) *ce.CodeError {
	streamKey := "mq:stream:" + cfg.StreamName
	group := cfg.ConsumerName

	err := c.rdb.XGroupCreateMkStream(c.ctx, streamKey, group, "0").Err()
	if err != nil && !isBusyGroupErr(err) {
		return mq.MQStreamError.Msg("xgroup create: " + err.Error())
	}
	c.logger.Info("Redis stream group ensured",
		logger.String("stream", streamKey),
		logger.String("group", group),
	)

	return nil
}

// IsConnected 检查连接状态
func (c *redisClient) IsConnected() bool {
	return c.rdb.Ping(c.ctx).Err() == nil
}

// Close 关闭连接
func (c *redisClient) Close() {
	c.cancel()
	if c.rdb != nil {
		_ = c.rdb.Close()
		c.logger.Info("Redis connection closed")
	}
}
