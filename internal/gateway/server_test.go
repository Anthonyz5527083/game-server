package gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Anthonyz5527083/game-server/internal/pb"
)

// startServer 起一个监听随机端口的 Server。测试结束时关停它,并检查 Serve 正常返回。
func startServer(t *testing.T, s *Server) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve returned %v", err)
		}
	})
	return ln.Addr().String()
}

// dial 连上服务端。整条连接带 5s deadline:任何一步卡住都让测试失败,而不是挂死。
func dial(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	nc, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	return nc, bufio.NewReader(nc)
}

// waitFor 轮询直到 cond 成立。断言等条件,不靠 Sleep 碰运气。
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

// closeRecorder 记录 OnClose 收到的原因。
type closeRecorder struct {
	mu      sync.Mutex
	reasons map[uint64]string
}

func (r *closeRecorder) handler() HandlerFuncs {
	return HandlerFuncs{Close: func(c *Conn, reason string) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.reasons == nil {
			r.reasons = make(map[uint64]string)
		}
		r.reasons[c.ID()] = reason
	}}
}

func (r *closeRecorder) only(t *testing.T) string {
	t.Helper()
	var got string
	waitFor(t, "OnClose", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, reason := range r.reasons {
			got = reason
		}
		return len(r.reasons) == 1
	})
	return got
}

// T1.4 Ping → Pong:seq 对得上,客户端时间原样带回。
func TestPingPong(t *testing.T) {
	addr := startServer(t, &Server{})
	nc, r := dial(t, addr)

	if err := WriteEnvelope(nc, ping(7, 123456)); err != nil {
		t.Fatal(err)
	}
	env, err := ReadEnvelope(r)
	if err != nil {
		t.Fatal(err)
	}
	pong := env.GetPong()
	if pong == nil {
		t.Fatalf("want Pong, got %T", env.GetPayload())
	}
	if env.GetSeq() != 7 || pong.GetClientTimeMs() != 123456 || pong.GetServerTimeMs() == 0 {
		t.Fatalf("bad pong: seq=%d %v", env.GetSeq(), pong)
	}
}

// T1.5 粘包:三帧一次 Write 发出去,服务端要逐帧切开、逐帧应答。
func TestCoalescedFrames(t *testing.T) {
	addr := startServer(t, &Server{})
	nc, r := dial(t, addr)

	var buf []byte
	for seq := uint32(1); seq <= 3; seq++ {
		f, err := Marshal(ping(seq, int64(seq)))
		if err != nil {
			t.Fatal(err)
		}
		buf = append(buf, f...)
	}
	if _, err := nc.Write(buf); err != nil {
		t.Fatal(err)
	}
	for seq := uint32(1); seq <= 3; seq++ {
		env, err := ReadEnvelope(r)
		if err != nil {
			t.Fatal(err)
		}
		if env.GetSeq() != seq {
			t.Fatalf("seq = %d, want %d", env.GetSeq(), seq)
		}
	}
}

// T1.6 半包:一帧拆两次写,拆分点在长度头中间,中间停一下。服务端要能拼回来。
func TestSplitFrame(t *testing.T) {
	addr := startServer(t, &Server{})
	nc, r := dial(t, addr)

	f, err := Marshal(ping(9, 9))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nc.Write(f[:2]); err != nil {
		t.Fatal(err)
	}
	// 这里的 Sleep 不是在等结果,是在制造「两段字节分两次到达」的条件。
	time.Sleep(20 * time.Millisecond)
	if _, err := nc.Write(f[2:]); err != nil {
		t.Fatal(err)
	}
	env, err := ReadEnvelope(r)
	if err != nil {
		t.Fatal(err)
	}
	if env.GetSeq() != 9 {
		t.Fatalf("seq = %d, want 9", env.GetSeq())
	}
}

// T1.7 idle 踢人:什么都不发,客户端在 timeout 附近读到 EOF,原因是 idle timeout。
func TestIdleTimeout(t *testing.T) {
	var rec closeRecorder
	const idle = 150 * time.Millisecond
	addr := startServer(t, &Server{IdleTimeout: idle, Handler: rec.handler()})
	nc, _ := dial(t, addr)

	start := time.Now()
	_, err := nc.Read(make([]byte, 1))
	elapsed := time.Since(start)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("read err = %v, want EOF", err)
	}
	if elapsed < idle || elapsed > idle+time.Second {
		t.Fatalf("kicked after %v, want about %v", elapsed, idle)
	}
	if got := rec.only(t); got != ReasonIdleTimeout {
		t.Fatalf("reason = %q, want %q", got, ReasonIdleTimeout)
	}
}

// 心跳能续命:每隔 idle/2 发一个 Ping,过了好几个 idle 周期连接还活着。
func TestPingKeepsConnAlive(t *testing.T) {
	const idle = 100 * time.Millisecond
	addr := startServer(t, &Server{IdleTimeout: idle})
	nc, r := dial(t, addr)
	for i := uint32(1); i <= 6; i++ {
		time.Sleep(idle / 2)
		if err := WriteEnvelope(nc, ping(i, 0)); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadEnvelope(r); err != nil {
			t.Fatalf("ping #%d: %v", i, err)
		}
	}
}

// T1.8 坏连接不连坐:A 发恶意长度头被断开,B 照常 ping/pong。
func TestBadFrameClosesOnlyThatConn(t *testing.T) {
	var rec closeRecorder
	addr := startServer(t, &Server{Handler: rec.handler()})
	bad, badR := dial(t, addr)
	good, goodR := dial(t, addr)

	if _, err := bad.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatal(err)
	}
	if _, err := badR.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("bad conn: read err = %v, want EOF", err)
	}
	if got := rec.only(t); got != ReasonProtocolError {
		t.Fatalf("reason = %q, want %q", got, ReasonProtocolError)
	}

	if err := WriteEnvelope(good, ping(1, 1)); err != nil {
		t.Fatal(err)
	}
	if env, err := ReadEnvelope(goodR); err != nil || env.GetPong() == nil {
		t.Fatalf("good conn broken: env=%v err=%v", env, err)
	}
}

// T1.9 并发 200 连接 ping/pong;全部关掉后 Count() 回到 0。
func TestConcurrentClients(t *testing.T) {
	s := &Server{}
	addr := startServer(t, s)

	const clients, pings = 200, 5
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- pingN(addr, uint32(i), pings)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "Count() == 0", func() bool { return s.Count() == 0 })
}

func pingN(addr string, id uint32, n int) error {
	nc, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return err
	}
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(nc)
	for i := range n {
		seq := id*1000 + uint32(i)
		if err := WriteEnvelope(nc, ping(seq, 0)); err != nil {
			return err
		}
		env, err := ReadEnvelope(r)
		if err != nil {
			return err
		}
		if env.GetSeq() != seq {
			return fmt.Errorf("client %d: seq = %d, want %d", id, env.GetSeq(), seq)
		}
	}
	return nil
}

// T1.10 优雅关停:ctx 取消后 Serve 返回,所有连接被关,每条连接都收到 OnClose(server shutdown),
// goroutine 数回到测试前的水平。
func TestShutdown(t *testing.T) {
	before := runtime.NumGoroutine()

	var mu sync.Mutex
	reasons := map[string]int{}
	s := &Server{Handler: HandlerFuncs{Close: func(_ *Conn, reason string) {
		mu.Lock()
		reasons[reason]++
		mu.Unlock()
	}}}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, ln) }()

	const n = 20
	readers := make([]*bufio.Reader, n)
	for i := range n {
		nc, r := dial(t, ln.Addr().String())
		readers[i] = r
		if err := WriteEnvelope(nc, ping(1, 0)); err != nil { // 确保每条连接都已被 Accept
			t.Fatal(err)
		}
		if _, err := ReadEnvelope(r); err != nil {
			t.Fatal(err)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after ctx cancel")
	}
	// Serve 返回时 OnClose 必须已经全部跑完(Serve 里等了 WaitGroup),所以这里不用轮询。
	mu.Lock()
	got := reasons[ReasonServerShutdown]
	mu.Unlock()
	if got != n {
		t.Fatalf("OnClose(server shutdown) called %d times, want %d (all reasons: %v)", got, n, reasons)
	}
	for i, r := range readers {
		if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
			t.Fatalf("client %d: read err = %v, want EOF", i, err)
		}
	}
	// 测试自己的客户端连接还开着(t.Cleanup 才关),但它们不占 goroutine。
	waitFor(t, "goroutines back to baseline", func() bool { return runtime.NumGoroutine() <= before })
}

// Handler 的三个回调按顺序来:OnOpen → OnMessage → OnClose,心跳不会交给 OnMessage。
func TestHandlerLifecycle(t *testing.T) {
	var mu sync.Mutex
	var events []string
	record := func(e string) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	addr := startServer(t, &Server{Handler: HandlerFuncs{
		Open:    func(*Conn) { record("open") },
		Message: func(_ *Conn, env *pb.Envelope) { record(fmt.Sprintf("msg %d", env.GetSeq())) },
		Close:   func(_ *Conn, reason string) { record("close: " + reason) },
	}})
	nc, r := dial(t, addr)
	if err := WriteEnvelope(nc, ping(1, 0)); err != nil { // 心跳:网关自己应答
		t.Fatal(err)
	}
	if _, err := ReadEnvelope(r); err != nil {
		t.Fatal(err)
	}
	// 一个非心跳消息(这里借 Pong 充当),应该交给 OnMessage。
	pong := &pb.Envelope{Seq: 2, Payload: &pb.Envelope_Pong{Pong: &pb.Pong{}}}
	if err := WriteEnvelope(nc, pong); err != nil {
		t.Fatal(err)
	}
	_ = nc.Close()

	want := []string{"open", "msg 2", "close: " + ReasonClientClosed}
	waitFor(t, "close event", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) == len(want)
	})
	mu.Lock()
	defer mu.Unlock()
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events = %q, want %q", events, want)
		}
	}
}

// D4 慢客户端:对端一直不读,服务端不停 Send,发送队列满时断开它,而不是阻塞发送方。
func TestSlowClientIsKicked(t *testing.T) {
	var rec closeRecorder
	opened := make(chan *Conn, 1)
	h := rec.handler()
	h.Open = func(c *Conn) { opened <- c }
	addr := startServer(t, &Server{SendQueueLen: 4, Handler: h})
	dial(t, addr) // 连上之后一个字节都不读

	c := <-opened
	big := make([]byte, 60<<10)
	start := time.Now()
	var err error
	// 内核的收发缓冲区先被塞满(几 MB),之后 writeLoop 卡在 Write 上,队列才会满。
	for i := 0; i < 10000 && err == nil; i++ {
		err = c.SendFrame(big)
	}
	if !errors.Is(err, ErrSendQueueFull) && !errors.Is(err, ErrConnClosed) {
		t.Fatalf("SendFrame never failed, last err = %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("sender was blocked by a slow client")
	}
	if got := rec.only(t); got != ReasonSendQueueFull {
		t.Fatalf("reason = %q, want %q", got, ReasonSendQueueFull)
	}
}

// SendAndClose:客户端先收到那一帧,再读到 EOF;OnClose 拿到的是传进去的原因。
func TestSendAndClose(t *testing.T) {
	var rec closeRecorder
	h := rec.handler()
	h.Message = func(c *Conn, env *pb.Envelope) {
		c.SendAndClose(&pb.Envelope{Seq: env.GetSeq(), Payload: &pb.Envelope_Pong{Pong: &pb.Pong{}}}, "bye")
	}
	addr := startServer(t, &Server{Handler: h})
	nc, r := dial(t, addr)
	if err := WriteEnvelope(nc, &pb.Envelope{Seq: 5, Payload: &pb.Envelope_Pong{Pong: &pb.Pong{}}}); err != nil {
		t.Fatal(err)
	}
	env, err := ReadEnvelope(r)
	if err != nil || env.GetSeq() != 5 {
		t.Fatalf("want last frame seq 5, got env=%v err=%v", env, err)
	}
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("read err = %v, want EOF", err)
	}
	if got := rec.only(t); got != "bye" {
		t.Fatalf("reason = %q, want %q", got, "bye")
	}
}
