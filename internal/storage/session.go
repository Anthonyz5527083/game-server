package storage

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrNoSession 表示 Redis 里没有这个玩家的会话。
var ErrNoSession = errors.New("storage: no session")

// Session 是一个在线玩家的会话。
//
// Owner 标识「哪个进程的哪条连接」持有这个会话(形如 "host-1234#17")。
// 删除时要比对它:同一账号在新连接上登录后,旧连接断开时不能把新会话删掉。
type Session struct {
	PlayerID uint64
	Owner    string
	LoginAt  time.Time
}

// Sessions 把会话存成 Redis hash:
//
//	gs:session:{playerID} → { owner, login_at(unix ms) },带 TTL
//
// TTL 是兜底:进程崩溃时 OnClose 来不及删,会话最多残留一个 TTL。
// 进程活着的时候由上层定期续期(PutMany)。
type Sessions struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewSessions(rdb *redis.Client, ttl time.Duration) *Sessions {
	return &Sessions{rdb: rdb, ttl: ttl}
}

func SessionKey(playerID uint64) string {
	return "gs:session:" + strconv.FormatUint(playerID, 10)
}

// Put 写入(或覆盖)一个会话,并重设 TTL。
func (s *Sessions) Put(ctx context.Context, sess Session) error {
	return s.PutMany(ctx, []Session{sess})
}

// PutMany 在一个 MULTI/EXEC 事务里写入多个会话:一次网络往返,
// 而且每个会话的 HSET 和 PEXPIRE 一起生效,不会出现「写进去了但没有 TTL」的永久 key。
func (s *Sessions) PutMany(ctx context.Context, sessions []Session) error {
	if len(sessions) == 0 {
		return nil
	}
	_, err := s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		for _, sess := range sessions {
			key := SessionKey(sess.PlayerID)
			p.HSet(ctx, key, "owner", sess.Owner, "login_at", sess.LoginAt.UnixMilli())
			p.PExpire(ctx, key, s.ttl)
		}
		return nil
	})
	return err
}

// deleteIfOwner 在 Redis 里原子地「比对 owner,相同才删」。
// 不能拆成 HGET + DEL 两条命令:两条之间别的进程可能已经写了新会话。
var deleteIfOwner = redis.NewScript(`
if redis.call("HGET", KEYS[1], "owner") == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

// Delete 删除 playerID 的会话,但只在 owner 匹配时删。返回是否真的删了。
func (s *Sessions) Delete(ctx context.Context, playerID uint64, owner string) (bool, error) {
	n, err := deleteIfOwner.Run(ctx, s.rdb, []string{SessionKey(playerID)}, owner).Int()
	return n == 1, err
}

// Get 读一个会话和它剩余的 TTL。主要给测试和排障用。
func (s *Sessions) Get(ctx context.Context, playerID uint64) (Session, time.Duration, error) {
	key := SessionKey(playerID)
	var all *redis.MapStringStringCmd
	var ttl *redis.DurationCmd
	_, err := s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		all = p.HGetAll(ctx, key)
		ttl = p.PTTL(ctx, key)
		return nil
	})
	if err != nil {
		return Session{}, 0, err
	}
	m := all.Val()
	if len(m) == 0 {
		return Session{}, 0, ErrNoSession
	}
	ms, _ := strconv.ParseInt(m["login_at"], 10, 64)
	return Session{PlayerID: playerID, Owner: m["owner"], LoginAt: time.UnixMilli(ms)}, ttl.Val(), nil
}
