// Package lobby 是「大厅」:接在网关的 Handler 上,管登录状态,并把业务消息分发出去。
//
// 一条连接的状态只有两种:未登录 → 已登录。未登录时只接受 Ping(网关自己处理)和 LoginReq。
//
// 权威数据在进程内存里(players 表:玩家 → 连接);Redis 里的会话是它的镜像,
// 给别的进程 / 运维看「谁在线」。镜像丢了(Redis 重启)会在下一次续期时补回来。见 D7。
package lobby

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Anthonyz5527083/game-server/internal/auth"
	"github.com/Anthonyz5527083/game-server/internal/gateway"
	"github.com/Anthonyz5527083/game-server/internal/pb"
	"github.com/Anthonyz5527083/game-server/internal/storage"
)

// 大厅自己用的关闭原因(网关的原因见 gateway.Reason*)。
const (
	ReasonLoginTimeout = "login timeout"
	ReasonBadToken     = "bad token"
	ReasonReplaced     = "replaced" // 同一账号在另一条连接上登录了
)

// SessionStore 是大厅对会话存储的全部要求。生产用 *storage.Sessions(Redis)。
type SessionStore interface {
	Put(ctx context.Context, s storage.Session) error
	PutMany(ctx context.Context, sessions []storage.Session) error
	Delete(ctx context.Context, playerID uint64, owner string) (bool, error)
}

type Config struct {
	Signer   *auth.Signer
	Sessions SessionStore

	// Node 标识本进程,和连接 ID 拼成会话的 owner。多实例部署时每个进程要不同。
	Node string
	// LoginTimeout:连上之后多久之内必须登录成功,否则踢掉。
	LoginTimeout time.Duration
	// SessionTTL:Redis 会话的 TTL。Run 每 TTL/3 续一次期,所以 Redis 里的会话最多比实际多活一个 TTL。
	SessionTTL time.Duration
	// StoreTimeout:单次会话存储操作的超时。
	StoreTimeout time.Duration

	Logger *slog.Logger
}

// Lobby 实现 gateway.Handler。
type Lobby struct {
	cfg Config
	log *slog.Logger

	mu      sync.Mutex
	clients map[uint64]*client // 连接 ID → 所有已打开的连接
	players map[uint64]*client // 玩家 ID → 已登录的连接
}

// client 是一条连接在大厅里的状态。字段都由 Lobby.mu 保护。
type client struct {
	conn       *gateway.Conn
	playerID   uint64 // 0 = 未登录
	loginAt    time.Time
	loginTimer *time.Timer
}

var _ gateway.Handler = (*Lobby)(nil)

func New(cfg Config) *Lobby {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Lobby{
		cfg:     cfg,
		log:     cfg.Logger,
		clients: make(map[uint64]*client),
		players: make(map[uint64]*client),
	}
}

// Run 定期给所有在线玩家的会话续期,直到 ctx 取消。
//
// 续期用一个 goroutine 批量做(一次 MULTI/EXEC 写全部在线玩家),而不是每个 Ping 打一次 Redis:
// 500 人每 20 秒一次网络往返,对比每秒 100 次往返。
// 续期用的是 Put(覆盖写),所以 Redis 重启丢了数据,下一轮会全部补回来。
func (l *Lobby) Run(ctx context.Context) {
	t := time.NewTicker(l.cfg.SessionTTL / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := l.refreshSessions(ctx); err != nil {
				l.log.Warn("refresh sessions failed", "err", err)
			}
		}
	}
}

func (l *Lobby) refreshSessions(ctx context.Context) error {
	l.mu.Lock()
	batch := make([]storage.Session, 0, len(l.players))
	for pid, cl := range l.players {
		batch = append(batch, storage.Session{PlayerID: pid, Owner: l.owner(cl.conn), LoginAt: cl.loginAt})
	}
	l.mu.Unlock()
	// 网络 I/O 放在锁外:Redis 慢的时候,不能让所有连接的登录、进出房间都排队等它。
	ctx, cancel := context.WithTimeout(ctx, l.cfg.StoreTimeout)
	defer cancel()
	return l.cfg.Sessions.PutMany(ctx, batch)
}

// Online 返回已登录的玩家数。
func (l *Lobby) Online() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.players)
}

func (l *Lobby) OnOpen(c *gateway.Conn) {
	cl := &client{conn: c}
	// 计时器回调在另一个 goroutine 上跑,所以要拿锁判断「到点时登录了没有」。
	cl.loginTimer = time.AfterFunc(l.cfg.LoginTimeout, func() {
		l.mu.Lock()
		loggedIn := cl.playerID != 0
		l.mu.Unlock()
		if !loggedIn {
			c.SendAndClose(kick(ReasonLoginTimeout), ReasonLoginTimeout)
		}
	})
	l.mu.Lock()
	l.clients[c.ID()] = cl
	l.mu.Unlock()
}

func (l *Lobby) OnMessage(c *gateway.Conn, env *pb.Envelope) {
	l.mu.Lock()
	cl := l.clients[c.ID()]
	l.mu.Unlock()
	if cl == nil {
		return
	}
	switch p := env.GetPayload().(type) {
	case *pb.Envelope_LoginReq:
		l.handleLogin(cl, env.GetSeq(), p.LoginReq)
	default:
		if _, ok := l.playerOf(cl); !ok {
			c.Logger().Info("message before login", "type", fmt.Sprintf("%T", p))
			c.CloseWithReason(gateway.ReasonProtocolError)
			return
		}
		c.Logger().Info("unexpected message", "type", fmt.Sprintf("%T", p))
		c.CloseWithReason(gateway.ReasonProtocolError)
	}
}

// OnClose 清理这条连接。和 OnMessage 在同一个 goroutine 上,不会和它并发(D4)。
func (l *Lobby) OnClose(c *gateway.Conn, reason string) {
	l.mu.Lock()
	cl := l.clients[c.ID()]
	delete(l.clients, c.ID())
	var pid uint64
	if cl != nil {
		if cl.loginTimer != nil {
			cl.loginTimer.Stop()
		}
		// 只有 players 表里登记的还是自己,才清理玩家状态。被顶号的旧连接走到这里时,
		// players[pid] 已经指向新连接了,它什么都不能动。
		if cl.playerID != 0 && l.players[cl.playerID] == cl {
			pid = cl.playerID
			delete(l.players, pid)
		}
	}
	l.mu.Unlock()
	if pid == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), l.cfg.StoreTimeout)
	defer cancel()
	// 按 owner 删:就算这里和新登录有竞争,也删不掉别的连接的会话。
	if _, err := l.cfg.Sessions.Delete(ctx, pid, l.owner(c)); err != nil {
		c.Logger().Warn("delete session failed, will expire by TTL", "player", pid, "err", err)
	}
	c.Logger().Info("player offline", "player", pid, "reason", reason)
}

func (l *Lobby) handleLogin(cl *client, seq uint32, req *pb.LoginReq) {
	c := cl.conn
	// 一条连接只能登录一次。被顶号的旧连接(playerID 还在,但 players 表里已经不是它)也算登录过,
	// 不然它可以在被断开之前再登录一次,把新连接顶掉。
	l.mu.Lock()
	loggedInBefore := cl.playerID != 0
	l.mu.Unlock()
	if loggedInBefore {
		_ = c.Send(loginResp(seq, pb.Code_ALREADY_LOGGED_IN, 0))
		return
	}

	pid, err := l.cfg.Signer.Verify(req.GetToken())
	if err != nil {
		code := pb.Code_BAD_TOKEN
		if errors.Is(err, auth.ErrExpired) {
			code = pb.Code_TOKEN_EXPIRED
		}
		c.Logger().Info("login rejected", "err", err)
		c.SendAndClose(loginResp(seq, code, 0), ReasonBadToken)
		return
	}

	// 先写 Redis,成功了再改内存:Redis 失败时内存里什么都没变,客户端可以直接重试。
	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), l.cfg.StoreTimeout)
	err = l.cfg.Sessions.Put(ctx, storage.Session{PlayerID: pid, Owner: l.owner(c), LoginAt: now})
	cancel()
	if err != nil {
		c.Logger().Warn("login: session store unavailable", "player", pid, "err", err)
		_ = c.Send(loginResp(seq, pb.Code_UNAVAILABLE, 0))
		return
	}

	l.mu.Lock()
	old := l.players[pid]
	l.players[pid] = cl
	cl.playerID, cl.loginAt = pid, now
	l.mu.Unlock()
	cl.loginTimer.Stop()

	// 顶号(D8):新登录踢掉旧连接。旧连接的 OnClose 看到 players[pid] 已经不是自己,不会清理。
	if old != nil && old != cl {
		old.conn.SendAndClose(kick(ReasonReplaced), ReasonReplaced)
	}
	_ = c.Send(loginResp(seq, pb.Code_OK, pid))
	c.Logger().Info("player online", "player", pid)
}

// playerOf 返回这条连接登录的玩家。被顶号的旧连接也算未登录。
func (l *Lobby) playerOf(cl *client) (uint64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if cl.playerID != 0 && l.players[cl.playerID] == cl {
		return cl.playerID, true
	}
	return 0, false
}

func (l *Lobby) owner(c *gateway.Conn) string {
	return fmt.Sprintf("%s#%d", l.cfg.Node, c.ID())
}

func kick(reason string) *pb.Envelope {
	return &pb.Envelope{Payload: &pb.Envelope_Kick{Kick: &pb.Kick{Reason: reason}}}
}

func loginResp(seq uint32, code pb.Code, pid uint64) *pb.Envelope {
	return &pb.Envelope{Seq: seq, Payload: &pb.Envelope_LoginResp{LoginResp: &pb.LoginResp{Code: code, PlayerId: pid}}}
}
