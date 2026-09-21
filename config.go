package mq

// Mode MQ 模式
type Mode string

const (
	ModeNATS  Mode = "nats"
	ModeRedis Mode = "redis"
)

// Config MQ 配置
type Config struct {
	Mode  Mode         `json:"mode" yaml:"mode"`
	NATS  *NATSConfig  `json:"nats,omitempty" yaml:"nats,omitempty"`
	Redis *RedisConfig `json:"redis,omitempty" yaml:"redis,omitempty"`
}

// NATSConfig NATS 连接配置
type NATSConfig struct {
	URL string `json:"url" yaml:"url"`
}

// RedisMQMode Redis MQ 子模式
type RedisMQMode string

const (
	RedisMQModeStream RedisMQMode = "streams" // Redis Streams（持久化、消费者组、ACK）
	RedisMQModePubSub RedisMQMode = "pubsub"  // Redis Pub/Sub + List（轻量级）
)

// RedisConfig Redis MQ 配置
type RedisConfig struct {
	Host     string      `json:"host" yaml:"host"`
	Port     int         `json:"port" yaml:"port"`
	Password string      `json:"password" yaml:"password"`
	DB       int         `json:"db" yaml:"db"`
	MQMode   RedisMQMode `json:"mqMode" yaml:"mqMode"` // streams / pubsub

	// 连接池
	PoolSize     int `json:"poolSize,omitempty" yaml:"poolSize,omitempty"`         // 最大连接数，默认 100
	MinIdleConns int `json:"minIdleConns,omitempty" yaml:"minIdleConns,omitempty"` // 最小空闲连接，默认 10
	MaxRetries   int `json:"maxRetries,omitempty" yaml:"maxRetries,omitempty"`     // 最大重试次数，默认 1

	// 超时（毫秒）
	DialTimeout  int `json:"dialTimeout,omitempty" yaml:"dialTimeout,omitempty"`   // 连接超时，默认 5000
	ReadTimeout  int `json:"readTimeout,omitempty" yaml:"readTimeout,omitempty"`   // 读超时，默认 3000
	WriteTimeout int `json:"writeTimeout,omitempty" yaml:"writeTimeout,omitempty"` // 写超时，默认 3000

	// 集群 + TLS
	IsCluster bool `json:"isCluster,omitempty" yaml:"isCluster,omitempty"` // 是否集群模式
	IsTLS     bool `json:"isTLS,omitempty" yaml:"isTLS,omitempty"`         // 是否启用 TLS

	// Stream 生产者
	StreamMaxLen int64 `json:"streamMaxLen,omitempty" yaml:"streamMaxLen,omitempty"` // Stream 最大长度，0 不裁剪
	BatchSize    int   `json:"batchSize,omitempty" yaml:"batchSize,omitempty"`       // 批量发送大小，<=1 同步单条发送
}

// QueueConfig 队列配置
type QueueConfig struct {
	// 通用配置
	StreamName    string `json:"streamName" yaml:"streamName"`
	StreamSubject string `json:"streamSubject" yaml:"streamSubject"`
	ConsumerName  string `json:"consumerName" yaml:"consumerName"`
	WorkerCount   int    `json:"workerCount" yaml:"workerCount"`
	AckWait       int    `json:"ackWait" yaml:"ackWait"` // 秒
	MaxDeliver    int    `json:"maxDeliver" yaml:"maxDeliver"`

	// Redis Streams 特有
	MaxLen    int64 `json:"maxLen,omitempty" yaml:"maxLen,omitempty"`       // Stream 最大长度
	ReadCount int   `json:"readCount,omitempty" yaml:"readCount,omitempty"` // 消费者每次读取条数，默认 100
}
