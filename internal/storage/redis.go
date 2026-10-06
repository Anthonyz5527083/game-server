package storage

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// NewRedis 建一个带超时的 Redis 客户端。go-redis 自带连接池,整个进程共用这一个。
//
// 超时设得比较短:Redis 在内网,正常一次往返是亚毫秒级。
// 它卡住时宁可快速失败、给客户端回 UNAVAILABLE,也不要让登录请求挂好几秒。
func NewRedis(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
		// 池子默认 10 × GOMAXPROCS 条连接。PoolTimeout:池子用满时最多等多久拿连接。
		PoolTimeout: time.Second,
	})
}
