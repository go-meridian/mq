// Package mq 提供统一的消息队列客户端接口，支持 NATS 和 Redis 两种实现。
//
// 使用示例：
//
//	// 导入实现包以注册工厂（重要！）
//	import (
//	    _ "github.com/go-meridian/mq/nats"
//	    _ "github.com/go-meridian/mq/redis"
//	)
//
//	// 创建客户端
//	cfg := &mq.Config{
//	    Mode: mq.ModeNATS,
//	    NATS: &mq.NATSConfig{URL: "nats://localhost:4222"},
//	}
//	client, err := mq.NewClient(cfg, logger)
//
//	// 发布消息
//	client.Publish("subject", data)
//
//	// 请求-回复（方式一：同步阻塞等待回复，类似 await）
//	resp, err := client.Request("subject", data, 5000)
//
//	// 请求-回复（方式二：异步，立即返回不阻塞，需要结果时再 await）
//	ch := client.RequestAsync("subject", data, 5000)
//	// ... 继续执行其他逻辑 ...
//	res := <-ch // await 回复
//	if res.Err != nil {
//	    // 超时/失败处理
//	}
//	resp := res.Data
//
//	// 订阅主题
//	sub, _ := client.Subscribe("subject", func(msg mq.Message) {
//	    fmt.Println(string(msg.Data()))
//	    // Request/Reply 模式：回复结果，调用方的 Request 收到该回复后继续执行
//	    msg.Respond(result)
//	})
//	defer sub.Unsubscribe()
//
//	// 订阅队列（消费者组，消息需显式 Ack/Nak）
//	queueSub, _ := client.SubscribeQueue(&mq.QueueConfig{
//	    StreamName:   "mystream",
//	    ConsumerName: "mygroup",
//	    WorkerCount:  4,
//	}, func(msg mq.Message) {
//	    if err := handle(msg.Data()); err != nil {
//	        // 处理失败：Nak 拒绝消息，触发重投递（Redis Streams 为 no-op，超时后自动重投递）
//	        msg.Nak()
//	        return
//	    }
//	    // 处理成功：Ack 确认消费，消息不再重投递
//	    msg.Ack()
//	})
//	defer queueSub.Unsubscribe()
package mq
