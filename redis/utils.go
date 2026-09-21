package redis

import (
	"crypto/tls"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// generateID 生成唯一 ID
func generateID() string {
	return uuid.New().String()
}

// jsonMarshal JSON 序列化
func jsonMarshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// parseDuration 解析时间间隔字符串（支持 ms 后缀）
func parseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		// 默认返回 5 秒
		return 5 * time.Second
	}
	return d
}

// isBusyGroupErr 检查错误是否为 "BUSYGROUP Consumer Group name already exists"
func isBusyGroupErr(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "BUSYGROUP Consumer Group name already exists"
}

// orDefault 返回 val，若 val <= 0 则返回 defaultVal
func orDefault(val, defaultVal int) int {
	if val <= 0 {
		return defaultVal
	}
	return val
}

// buildTLSConfig 构建 TLS 配置，isTLS 为 false 时返回 nil
func buildTLSConfig(isTLS bool) *tls.Config {
	if !isTLS {
		return nil
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
}
