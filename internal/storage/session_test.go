package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestRedis 起一个进程内的 miniredis(D10):不需要 Docker,测试跑得快、互不干扰。
func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := NewRedis(mr.Addr())
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

func TestSessionPutGet(t *testing.T) {
	_, rdb := newTestRedis(t)
	s := NewSessions(rdb, time.Minute)
	ctx := context.Background()

	login := time.UnixMilli(1_700_000_000_123)
	if err := s.Put(ctx, Session{PlayerID: 7, Owner: "node#1", LoginAt: login}); err != nil {
		t.Fatal(err)
	}
	got, ttl, err := s.Get(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	if got.Owner != "node#1" || !got.LoginAt.Equal(login) {
		t.Fatalf("got %+v", got)
	}
	if ttl <= 0 || ttl > time.Minute {
		t.Fatalf("ttl = %v, want (0, 1m]", ttl)
	}
}

func TestSessionExpires(t *testing.T) {
	mr, rdb := newTestRedis(t)
	s := NewSessions(rdb, time.Minute)
	ctx := context.Background()
	if err := s.Put(ctx, Session{PlayerID: 7, Owner: "a", LoginAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(61 * time.Second)
	if _, _, err := s.Get(ctx, 7); !errors.Is(err, ErrNoSession) {
		t.Fatalf("err = %v, want ErrNoSession", err)
	}
}

// 同一账号换了连接:旧 owner 删不掉新会话,新 owner 能删。
func TestSessionDeleteChecksOwner(t *testing.T) {
	_, rdb := newTestRedis(t)
	s := NewSessions(rdb, time.Minute)
	ctx := context.Background()
	if err := s.Put(ctx, Session{PlayerID: 7, Owner: "new", LoginAt: time.Now()}); err != nil {
		t.Fatal(err)
	}

	deleted, err := s.Delete(ctx, 7, "old")
	if err != nil || deleted {
		t.Fatalf("Delete(old) = %v, %v; want false, nil", deleted, err)
	}
	if _, _, err := s.Get(ctx, 7); err != nil {
		t.Fatalf("session gone after Delete(old): %v", err)
	}

	deleted, err = s.Delete(ctx, 7, "new")
	if err != nil || !deleted {
		t.Fatalf("Delete(new) = %v, %v; want true, nil", deleted, err)
	}
	if _, _, err := s.Get(ctx, 7); !errors.Is(err, ErrNoSession) {
		t.Fatalf("err = %v, want ErrNoSession", err)
	}
}

func TestSessionPutManyRefreshesTTL(t *testing.T) {
	mr, rdb := newTestRedis(t)
	s := NewSessions(rdb, time.Minute)
	ctx := context.Background()
	batch := []Session{{PlayerID: 1, Owner: "a"}, {PlayerID: 2, Owner: "b"}}
	if err := s.PutMany(ctx, batch); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(50 * time.Second)
	if err := s.PutMany(ctx, batch); err != nil { // 续期
		t.Fatal(err)
	}
	mr.FastForward(50 * time.Second) // 距离第一次写入 100 s,但距离续期只有 50 s
	for _, id := range []uint64{1, 2} {
		if _, _, err := s.Get(ctx, id); err != nil {
			t.Fatalf("player %d: %v", id, err)
		}
	}
}

// Redis 挂了,操作要返回错误(而不是卡住)。
func TestSessionRedisDown(t *testing.T) {
	mr, rdb := newTestRedis(t)
	s := NewSessions(rdb, time.Minute)
	mr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Put(ctx, Session{PlayerID: 1, Owner: "a"}); err == nil {
		t.Fatal("Put succeeded with Redis down")
	}
}
