package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Anthonyz5527083/game-server/internal/pb"
)

// Handler 接收连接的生命周期事件和非心跳消息。
//
// 三个方法都在该连接的读 goroutine 上按顺序调用:OnOpen 一次 → OnMessage 若干次 → OnClose 一次。
// 所以同一条连接的回调之间不用加锁;不同连接的回调是并发的。
// 回调里别做耗时操作,否则这条连接的下一帧要等它返回才会被读。
type Handler interface {
	OnOpen(c *Conn)
	OnMessage(c *Conn, env *pb.Envelope)
	// OnClose 在连接已经关闭之后调用,reason 是第一个关闭它的原因。
	OnClose(c *Conn, reason string)
}

// HandlerFuncs 用函数拼一个 Handler,nil 的字段表示不关心这个事件。测试和简单场景用。
type HandlerFuncs struct {
	Open    func(c *Conn)
	Message func(c *Conn, env *pb.Envelope)
	Close   func(c *Conn, reason string)
}

func (h HandlerFuncs) OnOpen(c *Conn) {
	if h.Open != nil {
		h.Open(c)
	}
}

func (h HandlerFuncs) OnMessage(c *Conn, env *pb.Envelope) {
	if h.Message != nil {
		h.Message(c, env)
	}
}

func (h HandlerFuncs) OnClose(c *Conn, reason string) {
	if h.Close != nil {
		h.Close(c, reason)
	}
}

// DefaultSendQueueLen 是每条连接发送队列的默认容量(帧数)。
// 20Hz 快照 + 少量应答,256 帧大约能扛住客户端 10 秒不读。
const DefaultSendQueueLen = 256

// Server 是 TCP 网关。字段为零值时使用默认行为;Serve 开始后不要再改字段。
type Server struct {
	Addr string

	// IdleTimeout:超过这么久没收到任何帧就踢掉。0 表示永不踢。
	IdleTimeout time.Duration
	// WriteTimeout:单帧写出的最长时间,防止对端不收包把 writeLoop 挂死。0 表示不限。
	WriteTimeout time.Duration
	// SendQueueLen:每条连接的发送队列容量,满了就踢掉这个慢客户端。0 表示 DefaultSendQueueLen。
	SendQueueLen int

	Handler Handler
	Logger  *slog.Logger // nil 时用 slog.Default()

	mu     sync.Mutex
	conns  map[uint64]*Conn
	nextID atomic.Uint64
	wg     sync.WaitGroup // 所有连接的读 / 写 goroutine
}

// ListenAndServe 监听 Addr 并阻塞服务,直到 ctx 被取消(返回 nil)或监听出错。
func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

// Serve 在给定的 listener 上服务。ctx 取消后:关 listener → 关所有连接 →
// 等所有连接的 goroutine(包括 Handler.OnClose)跑完 → 返回 nil。
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	s.mu.Lock()
	if s.conns == nil {
		s.conns = make(map[uint64]*Conn)
	}
	s.mu.Unlock()
	// 这条日志在 Listen 成功之后才打:打早了等于对运维撒谎。
	s.logger().Info("gateway listening", "addr", ln.Addr().String())

	// Accept 不接受 ctx。ctx 取消 → 关掉 listener → 阻塞中的 Accept 立刻返回错误 → 跳出循环。
	stop := context.AfterFunc(ctx, func() { _ = ln.Close() })
	defer stop()

	var backoff time.Duration
	for {
		nc, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}
			// 其它错误(典型的是 fd 用尽 EMFILE)不该让整个服务退出:退避后重试。
			// net/http.Server 的 Accept 循环也是这么做的。
			backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
			s.logger().Warn("accept error, retrying", "err", err, "backoff", backoff)
			time.Sleep(backoff)
			continue
		}
		backoff = 0
		s.handle(nc)
	}

	s.closeAll()
	s.wg.Wait() // OnClose 里可能还在清理会话,等它们做完再让进程退出
	return nil
}

// handle 给新连接建 Conn,登记之后起读 / 写两个 goroutine。
// goroutine-per-connection 是 Go 的惯用模型:500 连接 × 2 个 goroutine,每个起步 2 KiB 栈;
// 调度由 runtime 的 netpoller(Linux 上就是 epoll)完成,不用自己写 Reactor。
func (s *Server) handle(nc net.Conn) {
	c := &Conn{
		id:     s.nextID.Add(1),
		nc:     nc,
		srv:    s,
		sendCh: make(chan outFrame, s.sendQueueLen()),
		closed: make(chan struct{}),
	}
	c.log = s.logger().With("conn", c.id, "remote", nc.RemoteAddr().String())

	s.mu.Lock()
	s.conns[c.id] = c
	online := len(s.conns)
	s.mu.Unlock()

	c.log.Info("conn accepted", "online", online)
	s.wg.Add(2)
	go c.writeLoop()
	go c.readLoop()
}

func (s *Server) removeConn(c *Conn) {
	s.mu.Lock()
	delete(s.conns, c.id)
	s.mu.Unlock()
}

// Count 返回当前连接数。Lab 5 的在线连接数指标从这里取。
func (s *Server) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

func (s *Server) closeAll() {
	s.mu.Lock()
	conns := make([]*Conn, 0, len(s.conns))
	for _, c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	// 在锁外关:closeWith 会回调 removeConn 拿同一把锁,锁内关会死锁。
	for _, c := range conns {
		c.CloseWithReason(ReasonServerShutdown)
	}
}

func (s *Server) sendQueueLen() int {
	if s.SendQueueLen > 0 {
		return s.SendQueueLen
	}
	return DefaultSendQueueLen
}

func (s *Server) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}
