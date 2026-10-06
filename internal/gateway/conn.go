package gateway

import (
	"bufio"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/Anthonyz5527083/game-server/internal/pb"
)

var (
	ErrConnClosed    = errors.New("gateway: connection closed")
	ErrSendQueueFull = errors.New("gateway: send queue full")
)

// 关闭原因。日志和 Handler.OnClose 都用它们,上层也可以传自己的原因(比如 "replaced")。
const (
	ReasonClientClosed   = "client closed"   // 对端正常断开(EOF / RST)
	ReasonIdleTimeout    = "idle timeout"    // IdleTimeout 内没收到任何帧
	ReasonProtocolError  = "protocol error"  // 帧超限、零长度、截断、解码失败
	ReasonServerShutdown = "server shutdown" // Serve 的 ctx 被取消
	ReasonSendQueueFull  = "send queue full" // 慢客户端:发送队列满了
	ReasonIOError        = "io error"        // 其它读写错误
	ReasonKicked         = "kicked"          // Conn.Close 的默认原因
)

// Conn 是一条已接受的客户端 TCP 连接。
//
// 并发模型:每条连接两个 goroutine。
//   - readLoop 独占读端:解帧 → 解码 → 分发;Handler 的三个回调都在它上面按顺序执行。
//   - writeLoop 独占写端:从 sendCh 取帧,顺序写出。
//
// 任何 goroutine(房间广播、tick 循环)都可以调 Send,它只往 sendCh 投递;
// 真正的 Write 只发生在 writeLoop 里,所以 net.Conn 不用加锁,两帧的字节也不会交错。
type Conn struct {
	id  uint64
	nc  net.Conn
	srv *Server
	log *slog.Logger

	sendCh    chan outFrame
	closed    chan struct{}
	closeOnce sync.Once
	reason    string // 只在 closeOnce 里写一次;Once 保证别的 goroutine 之后读到的是写完的值
}

// outFrame 是发送队列里的一项。closeAfter 非空表示「写完这一帧就断开」。
type outFrame struct {
	data       []byte
	closeAfter string
}

// ID 是本进程内递增的连接编号,只用于日志和索引,不是玩家 ID。
func (c *Conn) ID() uint64 { return c.id }

func (c *Conn) RemoteAddr() net.Addr { return c.nc.RemoteAddr() }

// Logger 返回带 conn / remote 字段的日志器,上层打日志时用它,能和网关日志对上。
func (c *Conn) Logger() *slog.Logger { return c.log }

// Done 在连接关闭时被 close。上层可以 select 它来感知断线。
func (c *Conn) Done() <-chan struct{} { return c.closed }

// Send 编码 env 并投递到发送队列。不阻塞:队列满了就判定为慢客户端,断开它。
func (c *Conn) Send(env *pb.Envelope) error {
	frame, err := Marshal(env)
	if err != nil {
		return err
	}
	return c.SendFrame(frame)
}

// SendFrame 投递一帧已经编码好的字节。广播时给每条连接传同一个 slice,不复制;
// 所以调用方交出 frame 之后不能再改它。
//
// 队列满时选「断开」,不选「阻塞」也不选「丢帧」(D4):
// 阻塞会让广播方被一个慢客户端拖住,房间里其他人一起卡;
// 丢帧对请求 / 应答类消息不安全,客户端会永远等不到应答。
func (c *Conn) SendFrame(frame []byte) error {
	select {
	case <-c.closed:
		return ErrConnClosed
	default:
	}
	select {
	case c.sendCh <- outFrame{data: frame}:
		return nil
	default:
		c.closeWith(ReasonSendQueueFull, nil)
		return ErrSendQueueFull
	}
}

// SendAndClose 先把 env 写出去,再以 reason 断开。用于「告诉客户端为什么踢你」:
// 直接 Close 的话,队列里还没写出去的帧会一起丢掉,客户端只看到连接断了。
//
// 实现:往队列里放一个带 closeAfter 标记的帧,writeLoop 写完它就断开。
// 它前面排队的帧照常先写出去(队列是 FIFO)。队列满了就退化成立刻断开。
func (c *Conn) SendAndClose(env *pb.Envelope, reason string) {
	frame, err := Marshal(env)
	if err != nil {
		c.closeWith(reason, err)
		return
	}
	select {
	case <-c.closed:
	case c.sendCh <- outFrame{data: frame, closeAfter: reason}:
	default:
		c.closeWith(reason, nil)
	}
}

// Close 立刻断开连接,原因记为 ReasonKicked。幂等。
func (c *Conn) Close() error {
	c.CloseWithReason(ReasonKicked)
	return nil
}

// CloseWithReason 立刻断开连接。幂等,只有第一次调用的原因会被记下。
func (c *Conn) CloseWithReason(reason string) { c.closeWith(reason, nil) }

func (c *Conn) closeWith(reason string, err error) {
	c.closeOnce.Do(func() {
		c.reason = reason
		close(c.closed)  // 通知 writeLoop 退出,让 Send 立刻返回 ErrConnClosed
		_ = c.nc.Close() // 让阻塞在 Read 上的 readLoop 立刻返回错误
		c.srv.removeConn(c)
		if err != nil {
			c.log.Info("conn closed", "reason", reason, "err", err)
		} else {
			c.log.Info("conn closed", "reason", reason)
		}
	})
}

// readLoop 是连接的「主 goroutine」:OnOpen → 循环读帧 → OnClose。
// 因为三个回调都在这一个 goroutine 上顺序执行,上层可以确定:
// 同一条连接的 OnMessage 不会和它自己的 OnClose 并发,OnClose 也只会调一次。
func (c *Conn) readLoop() {
	defer c.srv.wg.Done()
	h := c.srv.Handler
	if h != nil {
		h.OnOpen(c)
	}
	reason, err := c.readFrames(h)
	c.closeWith(reason, err) // 如果别人已经关过了,这里什么都不做
	if h != nil {
		h.OnClose(c, c.reason)
	}
}

func (c *Conn) readFrames(h Handler) (string, error) {
	// bufio 把多次小 read 合并成一次大 read;本项目的帧都很小,4 KiB 足够。
	r := bufio.NewReaderSize(c.nc, 4<<10)
	for {
		if c.srv.IdleTimeout > 0 {
			// 每收到一帧就把 deadline 往后推:只要 IdleTimeout 内有任何帧(含 Ping)到达就不踢。
			// SetReadDeadline 不是系统调用,只改 runtime netpoller 里的定时器,每帧调一次开销可以忽略。
			if err := c.nc.SetReadDeadline(time.Now().Add(c.srv.IdleTimeout)); err != nil {
				return ReasonIOError, err
			}
		}
		body, err := ReadFrame(r)
		if err != nil {
			return classifyReadErr(err), err
		}
		env, err := Unmarshal(body)
		if err != nil {
			return ReasonProtocolError, err
		}
		c.dispatch(h, env)
	}
}

// classifyReadErr 把读错误翻译成关闭原因。
func classifyReadErr(err error) string {
	var ne net.Error
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, syscall.ECONNRESET):
		return ReasonClientClosed
	case errors.As(err, &ne) && ne.Timeout():
		return ReasonIdleTimeout
	case errors.Is(err, ErrFrameTooLarge), errors.Is(err, ErrEmptyFrame), errors.Is(err, io.ErrUnexpectedEOF):
		return ReasonProtocolError
	default:
		// 包括 net.ErrClosed:别的 goroutine 已经关了连接,readLoop 被唤醒。
		// 这种情况下 closeWith 不会覆盖已经记下的原因。
		return ReasonIOError
	}
}

// dispatch 分发一帧:心跳由网关自己应答,其余交给上层 Handler。
// 运行在读 goroutine 上:Handler 卡多久,这条连接就多久读不到下一帧(不影响其他连接)。
func (c *Conn) dispatch(h Handler, env *pb.Envelope) {
	if p, ok := env.GetPayload().(*pb.Envelope_Ping); ok {
		_ = c.Send(&pb.Envelope{
			Seq: env.GetSeq(),
			Payload: &pb.Envelope_Pong{Pong: &pb.Pong{
				ClientTimeMs: p.Ping.GetClientTimeMs(),
				ServerTimeMs: time.Now().UnixMilli(),
			}},
		}) // 失败只可能是连接正在关闭,不用处理
		return
	}
	if h == nil {
		c.log.Debug("no handler, message dropped", "seq", env.GetSeq())
		return
	}
	h.OnMessage(c, env)
}

func (c *Conn) writeLoop() {
	defer c.srv.wg.Done()
	for {
		select {
		case f := <-c.sendCh:
			if c.srv.WriteTimeout > 0 {
				_ = c.nc.SetWriteDeadline(time.Now().Add(c.srv.WriteTimeout))
			}
			// 一帧一次 Write。Lab 4 广播 20Hz 时,如果 write 系统调用成了瓶颈,
			// 可以在这里把队列里积压的多帧合并成一次 Write,再压测对比。
			if _, err := c.nc.Write(f.data); err != nil {
				c.closeWith(ReasonIOError, err)
				return
			}
			if f.closeAfter != "" { // SendAndClose 的那一帧已经写出去了
				c.closeWith(f.closeAfter, nil)
				return
			}
		case <-c.closed:
			return
		}
	}
}
