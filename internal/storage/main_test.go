package storage

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

type nopRedisLogger struct{}

func (nopRedisLogger) Printf(context.Context, string, ...any) {}

// Redis 宕机的测试会让 go-redis 打一串重连日志,淹没失败信息。
func TestMain(m *testing.M) {
	redis.SetLogger(nopRedisLogger{})
	os.Exit(m.Run())
}
