package nats

import (
	"os"
	"testing"

	"github.com/go-meridian/logger"
	"github.com/go-meridian/mq"
	natsLib "github.com/nats-io/nats.go"
)

// 本文件为集成测试，需要可连接的 NATS 服务器：
// 通过 NATS_URL 环境变量指定地址（默认 nats://127.0.0.1:4222），服务器不可用时自动跳过。

func TestMain(m *testing.M) {
	logDir, err := os.MkdirTemp("", "mq-nats-test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(logDir)

	if _, err := logger.Init(&logger.Config{Level: "error", LogDir: logDir, LogFile: "test"}); err != nil {
		panic(err)
	}
	defer logger.Close()

	os.Exit(m.Run())
}

func newTestClient(t *testing.T) mq.MQClient {
	t.Helper()

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = natsLib.DefaultURL
	}
	client, err := NewNATSClient(&mq.NATSConfig{URL: url})
	if err != nil {
		t.Skipf("跳过：NATS 服务器不可用（%s）: %v", url, err)
	}
	t.Cleanup(client.Close)
	return client
}

// TestRequestReply 验证 Request 阻塞等待 Respond 回复的完整链路
func TestRequestReply(t *testing.T) {
	client := newTestClient(t)

	sub, ce := client.Subscribe("test.echo", func(msg mq.Message) {
		if err := msg.Respond(append([]byte("echo:"), msg.Data()...)); err != nil {
			t.Errorf("Respond 失败: %v", err)
		}
	})
	if ce != nil {
		t.Fatalf("Subscribe 失败: %s", ce.Error())
	}
	defer sub.Unsubscribe()

	resp, ce := client.Request("test.echo", []byte("hello"), 3000)
	if ce != nil {
		t.Fatalf("Request 失败: %s", ce.Error())
	}
	if string(resp) != "echo:hello" {
		t.Fatalf("回复内容不符: got %q", resp)
	}
}

// TestRequestTimeout 验证无响应方时 Request 返回错误（超时或 no responders）
func TestRequestTimeout(t *testing.T) {
	client := newTestClient(t)

	resp, ce := client.Request("test.no.responder", []byte("hello"), 300)
	if ce == nil {
		t.Fatalf("期望错误，实际收到回复: %q", resp)
	}
}

// TestRequestAsync 验证异步请求：发出后不阻塞，需要结果时从通道 await
func TestRequestAsync(t *testing.T) {
	client := newTestClient(t)

	sub, ce := client.Subscribe("test.echo.async", func(msg mq.Message) {
		if err := msg.Respond(append([]byte("echo:"), msg.Data()...)); err != nil {
			t.Errorf("Respond 失败: %v", err)
		}
	})
	if ce != nil {
		t.Fatalf("Subscribe 失败: %s", ce.Error())
	}
	defer sub.Unsubscribe()

	// 不等待回复，同时发出多个请求（并发进行，互不阻塞）
	ch1 := client.RequestAsync("test.echo.async", []byte("hello"), 3000)
	ch2 := client.RequestAsync("test.echo.async", []byte("world"), 3000)

	// 需要结果时再 await
	res1 := <-ch1
	res2 := <-ch2
	if res1.Err != nil || res2.Err != nil {
		t.Fatalf("RequestAsync 失败: %v, %v", res1.Err, res2.Err)
	}
	if string(res1.Data) != "echo:hello" || string(res2.Data) != "echo:world" {
		t.Fatalf("回复内容不符: got %q, %q", res1.Data, res2.Data)
	}
}

// TestRequestAsyncError 验证异步请求失败时通过结果通道返回错误
func TestRequestAsyncError(t *testing.T) {
	client := newTestClient(t)

	ch := client.RequestAsync("test.no.responder", []byte("hello"), 300)
	res := <-ch
	if res.Err == nil {
		t.Fatalf("期望错误，实际收到回复: %q", res.Data)
	}
}
