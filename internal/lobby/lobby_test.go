package lobby

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Anthonyz5527083/game-server/internal/auth"
	"github.com/Anthonyz5527083/game-server/internal/gateway"
	"github.com/Anthonyz5527083/game-server/internal/pb"
	"github.com/Anthonyz5527083/game-server/internal/storage"
)

var showLog = flag.Bool("log", false, "打印日志(默认丢弃)")

func TestMain(m *testing.M) {
	flag.Parse()
	if !*showLog {
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
		redis.SetLogger(nopRedisLogger{}) // Redis 宕机的测试会让 go-redis 打一串重连日志
	}
	os.Exit(m.Run())
}

// ---------- 测试环境:miniredis + 真的网关 + 大厅 ----------

type testEnv struct {
	t        *testing.T
	addr     string
	mr       *miniredis.Miniredis
	signer   *auth.Signer
	lobby    *Lobby
	sessions *storage.Sessions
	closes   *closeLog
	stop     func() // 关停网关,等所有 OnClose 跑完
}

func newEnv(t *testing.T, mutate func(*Config)) *testEnv {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := storage.NewRedis(mr.Addr())
	t.Cleanup(func() { _ = rdb.Close() })
	signer, err := auth.NewSigner([]byte("lobby-test-secret-0123"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Signer:       signer,
		Sessions:     storage.NewSessions(rdb, time.Minute),
		Node:         "test",
		LoginTimeout: 2 * time.Second,
		SessionTTL:   time.Minute,
		StoreTimeout: 500 * time.Millisecond,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	l := New(cfg)
	closes := &closeLog{}
	srv := &gateway.Server{Handler: closes.wrap(l)}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("Serve: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	return &testEnv{t: t, addr: ln.Addr().String(), mr: mr, signer: signer, lobby: l,
		sessions: cfg.Sessions.(*storage.Sessions), closes: closes, stop: stop}
}

func (e *testEnv) token(pid uint64) string {
	tok, err := e.signer.Issue(pid, time.Now().Add(time.Hour))
	if err != nil {
		e.t.Fatal(err)
	}
	return tok
}

// session 读 Redis 里的会话;没有就返回 ok=false。
func (e *testEnv) session(pid uint64) (storage.Session, time.Duration, bool) {
	s, ttl, err := e.sessions.Get(context.Background(), pid)
	if errors.Is(err, storage.ErrNoSession) {
		return s, 0, false
	}
	if err != nil {
		e.t.Fatal(err)
	}
	return s, ttl, true
}

// closeLog 记录每次 OnClose 的原因,测试用它断言「为什么断开」。
type closeLog struct {
	mu      sync.Mutex
	reasons []string
}

func (r *closeLog) wrap(h gateway.Handler) gateway.Handler {
	return gateway.HandlerFuncs{
		Open:    h.OnOpen,
		Message: h.OnMessage,
		Close: func(c *gateway.Conn, reason string) {
			h.OnClose(c, reason)
			r.mu.Lock()
			r.reasons = append(r.reasons, reason)
			r.mu.Unlock()
		},
	}
}

func (r *closeLog) waitFor(t *testing.T, reason string) {
	t.Helper()
	waitFor(t, "close reason "+reason, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, got := range r.reasons {
			if got == reason {
				return true
			}
		}
		return false
	})
}

type nopRedisLogger struct{}

func (nopRedisLogger) Printf(context.Context, string, ...any) {}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------- 测试客户端 ----------

type testClient struct {
	t   *testing.T
	nc  net.Conn
	r   *bufio.Reader
	seq uint32
}

func (e *testEnv) dial() *testClient {
	e.t.Helper()
	nc, err := net.DialTimeout("tcp", e.addr, time.Second)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = nc.Close() })
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	return &testClient{t: e.t, nc: nc, r: bufio.NewReader(nc)}
}

// send 填上自增的 seq 再发出去,返回这个 seq。
func (c *testClient) send(env *pb.Envelope) uint32 {
	c.t.Helper()
	c.seq++
	env.Seq = c.seq
	if err := gateway.WriteEnvelope(c.nc, env); err != nil {
		c.t.Fatal(err)
	}
	return c.seq
}

func (c *testClient) recv() *pb.Envelope {
	c.t.Helper()
	env, err := gateway.ReadEnvelope(c.r)
	if err != nil {
		c.t.Fatalf("recv: %v", err)
	}
	return env
}

func (c *testClient) expectEOF() {
	c.t.Helper()
	if env, err := gateway.ReadEnvelope(c.r); !errors.Is(err, io.EOF) {
		c.t.Fatalf("want EOF, got env=%v err=%v", env, err)
	}
}

func (c *testClient) login(token string) *pb.LoginResp {
	c.t.Helper()
	seq := c.send(&pb.Envelope{Payload: &pb.Envelope_LoginReq{LoginReq: &pb.LoginReq{Token: token}}})
	env := c.recv()
	if env.GetSeq() != seq || env.GetLoginResp() == nil {
		c.t.Fatalf("want LoginResp seq=%d, got %v", seq, env)
	}
	return env.GetLoginResp()
}

func (c *testClient) ping() {
	c.t.Helper()
	seq := c.send(&pb.Envelope{Payload: &pb.Envelope_Ping{Ping: &pb.Ping{}}})
	if env := c.recv(); env.GetSeq() != seq || env.GetPong() == nil {
		c.t.Fatalf("want Pong seq=%d, got %v", seq, env)
	}
}

// ---------- T2.x ----------

// T2.1 合法 token:LoginResp OK,Redis 里有会话且带 TTL。
func TestLoginOK(t *testing.T) {
	e := newEnv(t, nil)
	c := e.dial()
	resp := c.login(e.token(7))
	if resp.GetCode() != pb.Code_OK || resp.GetPlayerId() != 7 {
		t.Fatalf("resp = %v", resp)
	}
	s, ttl, ok := e.session(7)
	if !ok || ttl <= 0 || !strings.HasPrefix(s.Owner, "test#") {
		t.Fatalf("session = %+v ttl=%v ok=%v", s, ttl, ok)
	}
	if e.lobby.Online() != 1 {
		t.Fatalf("Online = %d, want 1", e.lobby.Online())
	}
}

// T2.2 token 被改了一个字符:回 BAD_TOKEN 并断开。
func TestLoginTamperedToken(t *testing.T) {
	e := newEnv(t, nil)
	tok := e.token(7)
	last := tok[len(tok)-1]
	tampered := tok[:len(tok)-1] + string(map[bool]byte{true: 'A', false: 'B'}[last != 'A'])
	c := e.dial()
	if code := c.login(tampered).GetCode(); code != pb.Code_BAD_TOKEN {
		t.Fatalf("code = %v, want BAD_TOKEN", code)
	}
	c.expectEOF()
	e.closes.waitFor(t, ReasonBadToken)
	if _, _, ok := e.session(7); ok {
		t.Fatal("session written for a bad token")
	}
}

// T2.3 过期 token:回 TOKEN_EXPIRED(和签名错区分开)并断开。
func TestLoginExpiredToken(t *testing.T) {
	e := newEnv(t, nil)
	tok, err := e.signer.Issue(7, time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	c := e.dial()
	if code := c.login(tok).GetCode(); code != pb.Code_TOKEN_EXPIRED {
		t.Fatalf("code = %v, want TOKEN_EXPIRED", code)
	}
	c.expectEOF()
}

// T2.4 没登录就发业务消息:断开,原因是 protocol error。
func TestMessageBeforeLogin(t *testing.T) {
	e := newEnv(t, nil)
	c := e.dial()
	c.ping() // 心跳在登录前也可以发
	c.send(&pb.Envelope{Payload: &pb.Envelope_LoginResp{LoginResp: &pb.LoginResp{}}})
	c.expectEOF()
	e.closes.waitFor(t, gateway.ReasonProtocolError)
}

// T2.5 连上不登录:到 login timeout 收到 Kick,然后断开。Ping 不算登录。
func TestLoginTimeout(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.LoginTimeout = 150 * time.Millisecond })
	c := e.dial()
	start := time.Now()
	c.ping()
	env := c.recv()
	if env.GetKick().GetReason() != ReasonLoginTimeout {
		t.Fatalf("want Kick(login timeout), got %v", env)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("kicked after %v, before the timeout", elapsed)
	}
	c.expectEOF()
	e.closes.waitFor(t, ReasonLoginTimeout)
}

// 登录成功后计时器要停掉:过了 login timeout 也不该被踢。
func TestLoginStopsTimer(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.LoginTimeout = 100 * time.Millisecond })
	c := e.dial()
	if code := c.login(e.token(7)).GetCode(); code != pb.Code_OK {
		t.Fatalf("code = %v", code)
	}
	time.Sleep(200 * time.Millisecond) // 制造「超过 login timeout」的条件
	c.ping()
}

// T2.6 顶号:同一账号在第二条连接上登录,第一条收到 Kick(replaced) 后断开,
// 而且它断开时不能把新连接的会话删掉。
func TestDuplicateLoginReplacesOld(t *testing.T) {
	e := newEnv(t, nil)
	a := e.dial()
	if code := a.login(e.token(7)).GetCode(); code != pb.Code_OK {
		t.Fatalf("a: code = %v", code)
	}
	b := e.dial()
	if code := b.login(e.token(7)).GetCode(); code != pb.Code_OK {
		t.Fatalf("b: code = %v", code)
	}
	if env := a.recv(); env.GetKick().GetReason() != ReasonReplaced {
		t.Fatalf("a: want Kick(replaced), got %v", env)
	}
	a.expectEOF()
	e.closes.waitFor(t, ReasonReplaced)

	if _, _, ok := e.session(7); !ok {
		t.Fatal("old connection's OnClose deleted the new session")
	}
	if e.lobby.Online() != 1 {
		t.Fatalf("Online = %d, want 1", e.lobby.Online())
	}
	b.ping() // 新连接不受影响
}

// T2.7 断线:会话立刻从 Redis 删掉。
func TestDisconnectDeletesSession(t *testing.T) {
	e := newEnv(t, nil)
	c := e.dial()
	if code := c.login(e.token(7)).GetCode(); code != pb.Code_OK {
		t.Fatalf("code = %v", code)
	}
	_ = c.nc.Close()
	waitFor(t, "session deleted", func() bool { _, _, ok := e.session(7); return !ok })
	if e.lobby.Online() != 0 {
		t.Fatalf("Online = %d, want 0", e.lobby.Online())
	}
}

// T2.8 Redis 挂了:登录回 UNAVAILABLE,连接和进程都还活着;Redis 回来后不用重启就能登录。
func TestLoginWhileRedisDown(t *testing.T) {
	e := newEnv(t, nil)
	e.mr.Close()
	c := e.dial()
	if code := c.login(e.token(7)).GetCode(); code != pb.Code_UNAVAILABLE {
		t.Fatalf("code = %v, want UNAVAILABLE", code)
	}
	c.ping() // 连接还在

	if err := e.mr.Restart(); err != nil {
		t.Fatal(err)
	}
	if code := c.login(e.token(7)).GetCode(); code != pb.Code_OK {
		t.Fatalf("after restart: code = %v, want OK", code)
	}
}

// Redis 重启丢了数据:下一轮续期把在线玩家的会话补回来。
func TestRefreshRestoresLostSessions(t *testing.T) {
	e := newEnv(t, nil)
	for _, pid := range []uint64{1, 2, 3} {
		if code := e.dial().login(e.token(pid)).GetCode(); code != pb.Code_OK {
			t.Fatalf("player %d: code = %v", pid, code)
		}
	}
	e.mr.FlushAll()
	if err := e.lobby.refreshSessions(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, pid := range []uint64{1, 2, 3} {
		if _, ttl, ok := e.session(pid); !ok || ttl <= 0 {
			t.Fatalf("player %d not restored (ttl=%v)", pid, ttl)
		}
	}
}

func TestLoginTwiceOnSameConn(t *testing.T) {
	e := newEnv(t, nil)
	c := e.dial()
	if code := c.login(e.token(7)).GetCode(); code != pb.Code_OK {
		t.Fatalf("code = %v", code)
	}
	if code := c.login(e.token(8)).GetCode(); code != pb.Code_ALREADY_LOGGED_IN {
		t.Fatalf("second login: code = %v, want ALREADY_LOGGED_IN", code)
	}
}

// 服务端关停:Serve 返回之前,所有在线玩家的会话都已经删掉。
func TestShutdownDeletesSessions(t *testing.T) {
	e := newEnv(t, nil)
	for _, pid := range []uint64{1, 2} {
		if code := e.dial().login(e.token(pid)).GetCode(); code != pb.Code_OK {
			t.Fatalf("player %d: code = %v", pid, code)
		}
	}
	e.stop()
	for _, pid := range []uint64{1, 2} {
		if _, _, ok := e.session(pid); ok {
			t.Fatalf("player %d: session still in Redis after shutdown", pid)
		}
	}
}
