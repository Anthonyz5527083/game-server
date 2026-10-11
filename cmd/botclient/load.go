package main

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Anthonyz5527083/game-server/internal/gateway"
	"github.com/Anthonyz5527083/game-server/internal/pb"
)

// load 是压测模式。分三段:
//  1. 准备:o.conns 个 bot 同时连接、登录、快速匹配,记录登录耗时;
//  2. 预热 o.warmup:开始收发,样本不计。刚建好的房间、刚分配的缓冲、GC 的第一轮都在这段过去;
//  3. 测量 o.dur:每个 bot 按 ticker 发 MoveInput 和 Ping。
//
// Ping 是 open-loop 的:按 ticker 发,不等上一个 Pong 回来。等回来再发(closed-loop)的话,
// 服务端卡住的那段时间里 bot 也停下来不发了,慢的那几个样本根本没机会被采到,
// P99 会好看得不真实(coordinated omission)。
//
// 统计口径(都只算测量窗口内到达的):
//   - Ping RTT:Pong 到达时刻 − 对应 Ping 的发送时刻,本地单调时钟;
//   - 快照间隔:同一个 bot 相邻两个快照的到达时间差;
//   - 到达率:收到的快照数 ÷ (bot 数 × 20 × 时长)。
func load(o options) error {
	if o.player == 0 {
		o.player = 1
	}
	signer, err := newSigner()
	if err != nil {
		return err
	}

	// ---------- 1. 准备 ----------
	fmt.Printf("setup: %d connections to %s\n", o.conns, o.addr)
	setupStart := time.Now()
	bots := make([]*bot, o.conns)
	logins := make([]time.Duration, o.conns)
	errs := make([]error, o.conns)
	var wg sync.WaitGroup
	for i := range o.conns {
		wg.Go(func() {
			pid := o.player + uint64(i)
			tok, err := signer.Issue(pid, time.Now().Add(time.Hour))
			if err == nil {
				bots[i], logins[i], err = setupBot(o, pid, tok)
			}
			errs[i] = err
		})
	}
	wg.Wait()

	var ready []*bot
	var loginOK []time.Duration
	rooms := map[uint32]int{}
	failed := 0
	for i, err := range errs {
		if err != nil {
			if failed == 0 {
				fmt.Printf("  first failure: player %d: %v\n", o.player+uint64(i), err)
			}
			failed++
			continue
		}
		ready = append(ready, bots[i])
		loginOK = append(loginOK, logins[i])
		rooms[bots[i].room]++
	}
	fmt.Printf("setup: %d ready, %d failed, %d rooms, took %v\n",
		len(ready), failed, len(rooms), time.Since(setupStart).Round(time.Millisecond))
	fmt.Println("  " + dist("login", loginOK))
	if len(ready) == 0 {
		return errors.New("no connection survived setup")
	}

	// ---------- 2 + 3. 预热 + 测量 ----------
	base := time.Now()
	win := window{start: o.warmup, end: o.warmup + o.dur}
	stop := make(chan struct{})
	var stopping atomic.Bool
	var rw sync.WaitGroup
	for _, b := range ready {
		rw.Go(func() { b.readLoop(base, win, &stopping) })
		rw.Go(func() { b.writeLoop(base, o, stop) })
	}
	fmt.Printf("warmup %v, then measuring %v\n", o.warmup, o.dur)
	time.Sleep(o.warmup + o.dur)
	stopping.Store(true)
	close(stop)
	for _, b := range ready {
		_ = b.nc.Close()
	}
	rw.Wait()

	// ---------- 汇总 ----------
	var rtts, gaps []time.Duration
	snaps, dropped := 0, 0
	for _, b := range ready {
		rtts = append(rtts, b.rtts...)
		gaps = append(gaps, b.gaps...)
		snaps += b.snaps
		if b.err != nil {
			if dropped == 0 {
				fmt.Printf("  first drop: player %d: %v\n", b.player, b.err)
			}
			dropped++
		}
	}
	secs := o.dur.Seconds()
	wantPings := float64(len(ready)*o.pingHz) * secs
	wantSnaps := float64(len(ready)) * 20 * secs // 服务端固定 20Hz
	fmt.Printf("result: %d connections, %d dropped during run\n", len(ready), dropped)
	fmt.Printf("  %s (expected ~%.0f)\n", dist("ping rtt", rtts), wantPings)
	fmt.Println("  " + dist("snapshot interval", gaps))
	fmt.Printf("  snapshots: %d / %.0f (%.3f%%)\n", snaps, wantSnaps, 100*float64(snaps)/wantSnaps)
	if failed > 0 || dropped > 0 {
		return fmt.Errorf("%d failed during setup, %d dropped during run", failed, dropped)
	}
	return nil
}

// window 是测量窗口,相对 base 的偏移。
type window struct{ start, end time.Duration }

func (w window) contains(t time.Duration) bool { return t >= w.start && t < w.end }

// bot 是压测里的一条连接。读、写各一个 goroutine;rtts / gaps / snaps / err 只由读 goroutine 写,
// 主 goroutine 在 rw.Wait() 之后才读,WaitGroup 提供 happens-before。
type bot struct {
	player uint64
	room   uint32
	nc     net.Conn
	r      *bufio.Reader

	// sentAt[seq % 256] 是这个 seq 的 Ping 的发送时刻(相对 base 的纳秒)。写 goroutine 写、读 goroutine 读,
	// 所以用 atomic。5Hz 下 256 格能记 51 秒,RTT 不可能长到让格子被覆盖。
	sentAt [256]atomic.Int64

	rtts, gaps []time.Duration
	snaps      int
	err        error
}

// setupBot 连接、登录、快速匹配,返回 bot 和登录耗时(发出 LoginReq 到收到 LoginResp)。
func setupBot(o options, pid uint64, tok string) (*bot, time.Duration, error) {
	nc, err := net.DialTimeout("tcp", o.addr, o.timeout)
	if err != nil {
		return nil, 0, err
	}
	c := &client{nc: nc, r: bufio.NewReader(nc), timeout: o.timeout, quiet: true}
	start := time.Now()
	if _, err := login(c, tok); err != nil {
		_ = nc.Close()
		return nil, 0, err
	}
	took := time.Since(start)
	info, err := quickMatch(c)
	if err != nil {
		_ = nc.Close()
		return nil, 0, err
	}
	_ = nc.SetDeadline(time.Time{}) // 之后读写各自设 deadline
	return &bot{player: pid, room: info.GetRoomId(), nc: nc, r: c.r}, took, nil
}

func (b *bot) readLoop(base time.Time, win window, stopping *atomic.Bool) {
	prevSnap := time.Duration(-1)
	for {
		// 快照每 50ms 一个,5 秒收不到任何东西说明服务端出问题了
		_ = b.nc.SetReadDeadline(time.Now().Add(5 * time.Second))
		env, err := gateway.ReadEnvelope(b.r)
		if err != nil {
			if !stopping.Load() {
				b.err = err
			}
			return
		}
		now := time.Since(base)
		switch {
		case env.GetSnapshot() != nil:
			if win.contains(now) {
				b.snaps++
				if prevSnap >= win.start {
					b.gaps = append(b.gaps, now-prevSnap)
				}
			}
			prevSnap = now
		case env.GetPong() != nil:
			if win.contains(now) {
				sent := time.Duration(b.sentAt[env.GetSeq()%uint32(len(b.sentAt))].Load())
				b.rtts = append(b.rtts, now-sent)
			}
		case env.GetKick() != nil:
			b.err = fmt.Errorf("kicked: %s", env.GetKick().GetReason())
			return
		}
	}
}

// writeLoop 按 ticker 发 MoveInput 和 Ping,直到 stop。方向每秒随机转一次,
// 场地有边界,走到边上会被服务端截住,不会走出去。
//
// 开始之前先等一个随机相位(0 到一个 Ping 周期之间)。所有 bot 的 ticker 是在同一个循环里建的,
// 不错开的话 500 个 bot 会在同一瞬间一起发,每 50ms 一波 500 条的突发:服务端和 bot 都得先把这一波
// 排队消化完,量到的 RTT 主要是排队时间,而且随连接数线性涨(实测 200 连接 p50 2.1ms、500 连接 4.5ms)。
// 真实玩家的发送时刻是随机的,压测客户端也应该是。
func (b *bot) writeLoop(base time.Time, o options, stop <-chan struct{}) {
	rng := rand.New(rand.NewPCG(b.player, 1))
	phase := time.NewTimer(time.Duration(rng.Int64N(int64(time.Second / time.Duration(o.pingHz)))))
	select {
	case <-stop:
		phase.Stop()
		return
	case <-phase.C:
	}
	move := time.NewTicker(time.Second / time.Duration(o.moveHz))
	ping := time.NewTicker(time.Second / time.Duration(o.pingHz))
	defer move.Stop()
	defer ping.Stop()
	angle := rng.Float64() * 2 * math.Pi
	var moveSeq, pingSeq uint32
	write := func(env *pb.Envelope) bool {
		_ = b.nc.SetWriteDeadline(time.Now().Add(5 * time.Second))
		return gateway.WriteEnvelope(b.nc, env) == nil // 失败时读 goroutine 会看到断线并记下原因
	}
	for {
		select {
		case <-stop:
			return
		case <-move.C:
			moveSeq++
			if moveSeq%uint32(o.moveHz) == 0 {
				angle += (rng.Float64() - 0.5) * math.Pi
			}
			if !write(&pb.Envelope{Payload: &pb.Envelope_MoveInput{MoveInput: &pb.MoveInput{
				Seq: moveSeq, DirX: float32(math.Cos(angle)), DirY: float32(math.Sin(angle)),
			}}}) {
				return
			}
		case <-ping.C:
			pingSeq++
			now := time.Since(base)
			b.sentAt[pingSeq%uint32(len(b.sentAt))].Store(int64(now))
			if !write(&pb.Envelope{Seq: pingSeq, Payload: &pb.Envelope_Ping{Ping: &pb.Ping{
				ClientTimeMs: base.Add(now).UnixMilli(),
			}}}) {
				return
			}
		}
	}
}
