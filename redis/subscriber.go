package redis

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-meridian/logger"
	"github.com/go-meridian/mq"
	"github.com/redis/go-redis/v9"
)

// redisPubSubSubscription Redis Pub/Sub 订阅实现
type redisPubSubSubscription struct {
	pubsub  *redis.PubSub
	subject string
	handler func(msg mq.Message)
	logger  *logger.Logger
	ctx     context.Context
	running bool
	mu      sync.Mutex
}

func (s *redisPubSubSubscription) start() {
	s.running = true
	go s.listen()
	s.logger.Info("Redis Pub/Sub subscription started", logger.String("subject", s.subject))
}

func (s *redisPubSubSubscription) listen() {
	ch := s.pubsub.Channel()
	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Pub/Sub listener stopped", logger.String("subject", s.subject))
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			m := &redisMessage{
				subject:   s.subject,
				data:      []byte(msg.Payload),
				timestamp: time.Now(),
			}
			s.handler(m)
		}
	}
}

func (s *redisPubSubSubscription) Unsubscribe() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		s.running = false
		return s.pubsub.Close()
	}
	return nil
}

func (s *redisPubSubSubscription) IsActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// redisStreamSubscription Redis Streams 订阅实现（无消费者组）
type redisStreamSubscription struct {
	rdb       redisCmd
	streamKey string
	subject   string
	handler   func(msg mq.Message)
	logger    *logger.Logger
	ctx       context.Context
	cancel    context.CancelFunc
	running   bool
	mu        sync.Mutex
	lastID    string
}

func (s *redisStreamSubscription) start() {
	ctx, cancel := context.WithCancel(s.ctx)
	s.ctx = ctx
	s.cancel = cancel
	s.running = true
	s.lastID = "0"

	go s.listen()
	s.logger.Info("Redis Stream subscription started", logger.String("stream", s.streamKey))
}

func (s *redisStreamSubscription) listen() {
	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Stream listener stopped", logger.String("stream", s.streamKey))
			return
		default:
			streams, err := s.rdb.XRead(s.ctx, &redis.XReadArgs{
				Streams: []string{s.streamKey, s.lastID},
				Block:   2 * time.Second,
				Count:   1,
			}).Result()

			if err == redis.Nil {
				continue
			}
			if err != nil {
				if s.ctx.Err() != nil {
					return
				}
				s.logger.Error("XRead error", logger.Error(err))
				continue
			}

			for _, stream := range streams {
				for _, xmsg := range stream.Messages {
					s.lastID = xmsg.ID
					data, _ := xmsg.Values["data"].(string)
					m := &redisMessage{
						subject:   s.subject,
						data:      []byte(data),
						timestamp: time.Now(),
					}
					s.handler(m)
				}
			}
		}
	}
}

func (s *redisStreamSubscription) Unsubscribe() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		s.cancel()
		s.running = false
	}
	return nil
}

func (s *redisStreamSubscription) IsActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// StreamStats 流统计信息
type StreamStats struct {
	StreamKey   string `json:"streamKey"`
	Group       string `json:"group"`
	Consumer    string `json:"consumer"`
	LastEntryID string `json:"lastEntryId"`
	LastReadID  string `json:"lastReadId"`
	Latency     int64  `json:"latency"` // 消息延迟（ID 时间戳差值，毫秒）
	Pending     int64  `json:"pending"` // 未确认消息数
	Total       int64  `json:"total"`   // 已消费总数
}

// redisQueueSubscription Redis Streams 消费者组订阅实现
type redisQueueSubscription struct {
	rdb       redisCmd
	streamKey string
	group     string
	consumer  string
	pool      *mq.WorkerPool
	logger    *logger.Logger
	ctx       context.Context
	cancel    context.CancelFunc
	running   bool
	mu        sync.Mutex
	readCount int
	lastRead  string
	total     int64
}

func (s *redisQueueSubscription) start() {
	ctx, cancel := context.WithCancel(s.ctx)
	s.ctx = ctx
	s.cancel = cancel
	s.running = true

	go s.listen()
	s.logger.Info("Redis queue subscription started",
		logger.String("stream", s.streamKey),
		logger.String("group", s.group),
		logger.String("consumer", s.consumer),
		logger.Int("readCount", s.readCount),
	)
}

// listen 消费消息，启动时先处理 backlog 中未确认的消息
func (s *redisQueueSubscription) listen() {
	checkBacklog := true

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Queue listener stopped",
				logger.String("stream", s.streamKey),
				logger.String("group", s.group),
			)
			return
		default:
			id := ">"
			if checkBacklog {
				id = "0" // 从头读取未确认消息
			}

			streams, err := s.rdb.XReadGroup(s.ctx, &redis.XReadGroupArgs{
				Group:    s.group,
				Consumer: s.consumer,
				Streams:  []string{s.streamKey, id},
				Block:    2 * time.Second,
				Count:    int64(s.readCount),
			}).Result()

			if err == redis.Nil {
				continue
			}
			if err != nil {
				if s.ctx.Err() != nil {
					return
				}
				s.logger.Error("XReadGroup error", logger.Error(err))
				continue
			}

			// backlog 读到空消息，说明之前未确认的消息已处理完，切换到新消息模式
			if checkBacklog && len(streams) > 0 && len(streams[0].Messages) == 0 {
				checkBacklog = false
				s.logger.Info("Backlog cleared, switching to new messages",
					logger.String("stream", s.streamKey),
					logger.String("group", s.group),
				)
				continue
			}

			for _, stream := range streams {
				for _, xmsg := range stream.Messages {
					data, _ := xmsg.Values["data"].(string)
					m := &redisStreamMessage{
						subject:   s.streamKey,
						streamKey: s.streamKey,
						group:     s.group,
						id:        xmsg.ID,
						data:      []byte(data),
						timestamp: time.Now(),
						ackFn: func(id string) error {
							return s.rdb.XAck(s.ctx, s.streamKey, s.group, id).Err()
						},
					}
					s.pool.Submit(m)

					s.mu.Lock()
					s.lastRead = xmsg.ID
					s.total++
					s.mu.Unlock()
				}
			}
		}
	}
}

// ClaimPending 转移超时未确认的消息到当前消费者（参考 BalloonService Group.xClaim）
func (s *redisQueueSubscription) ClaimPending(minIdle time.Duration) (int, error) {
	// 1. 获取等待队列摘要
	pending, err := s.rdb.XPending(s.ctx, s.streamKey, s.group).Result()
	if err != nil {
		return 0, err
	}
	if pending.Count == 0 {
		return 0, nil
	}

	// 2. 获取具体的 pending 消息
	pendingExt, err := s.rdb.XPendingExt(s.ctx, &redis.XPendingExtArgs{
		Stream:   s.streamKey,
		Group:    s.group,
		Start:    "-",
		End:      "+",
		Count:    100,
		Consumer: "",
	}).Result()
	if err != nil {
		return 0, err
	}

	// 3. 筛选超时的消息
	var ids []string
	for _, p := range pendingExt {
		if p.Idle >= minIdle {
			ids = append(ids, p.ID)
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}

	// 4. XCLAIM 转移
	claimed, err := s.rdb.XClaim(s.ctx, &redis.XClaimArgs{
		Stream:   s.streamKey,
		Group:    s.group,
		Consumer: s.consumer,
		MinIdle:  minIdle,
		Messages: ids,
	}).Result()
	if err != nil {
		return 0, err
	}

	// 5. 将 claimed 消息提交到 worker pool
	for _, xmsg := range claimed {
		data, _ := xmsg.Values["data"].(string)
		m := &redisStreamMessage{
			subject:   s.streamKey,
			streamKey: s.streamKey,
			group:     s.group,
			id:        xmsg.ID,
			data:      []byte(data),
			timestamp: time.Now(),
			ackFn: func(id string) error {
				return s.rdb.XAck(s.ctx, s.streamKey, s.group, id).Err()
			},
		}
		s.pool.Submit(m)
	}

	s.logger.Info("Claimed pending messages",
		logger.String("stream", s.streamKey),
		logger.String("group", s.group),
		logger.Int("claimed", len(claimed)),
	)

	return len(claimed), nil
}

// GetStats 获取流统计信息（参考 BalloonService Consumer.GetStatistic）
func (s *redisQueueSubscription) GetStats() (*StreamStats, error) {
	info, err := s.rdb.XInfoStream(s.ctx, s.streamKey).Result()
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	lastRead := s.lastRead
	total := s.total
	s.mu.Unlock()

	latency := int64(-1)
	if info.LastEntry.ID != "" && lastRead != "" && lastRead != "0" {
		entryStrs := strings.Split(info.LastEntry.ID, "-")
		readStrs := strings.Split(lastRead, "-")
		if len(entryStrs) >= 1 && len(readStrs) >= 1 {
			entryId, e1 := strconv.ParseInt(entryStrs[0], 10, 64)
			readId, e2 := strconv.ParseInt(readStrs[0], 10, 64)
			if e1 == nil && e2 == nil {
				latency = entryId - readId
			}
		}
	}

	pendingInfo, err := s.rdb.XPending(s.ctx, s.streamKey, s.group).Result()
	pending := int64(0)
	if err == nil {
		pending = pendingInfo.Count
	}

	return &StreamStats{
		StreamKey:   s.streamKey,
		Group:       s.group,
		Consumer:    s.consumer,
		LastEntryID: info.LastEntry.ID,
		LastReadID:  lastRead,
		Latency:     latency,
		Pending:     pending,
		Total:       total,
	}, nil
}

func (s *redisQueueSubscription) Unsubscribe() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		s.cancel()
		s.pool.Stop()
		s.running = false
	}
	return nil
}

func (s *redisQueueSubscription) IsActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}
