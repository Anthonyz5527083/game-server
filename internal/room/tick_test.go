package room

import (
	"bytes"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Anthonyz5527083/game-server/internal/gateway"
	"github.com/Anthonyz5527083/game-server/internal/pb"
	"github.com/Anthonyz5527083/game-server/internal/sim"
)

const testTick = 50 * time.Millisecond

func newTickManager(t *testing.T) *Manager {
	t.Helper()
	m := NewManager(Config{Capacity: 8, TickInterval: testTick, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	t.Cleanup(m.Close)
	return m
}

// snapConn 记录收到的每个快照和到达时间;房间事件忽略。
type snapConn struct {
	mu    sync.Mutex
	snaps []*pb.Snapshot
	at    []time.Time
}

func (s *snapConn) SendFrame(frame []byte) error {
	env, err := gateway.ReadEnvelope(bytes.NewReader(frame))
	if err != nil {
		panic(err)
	}
	if snap := env.GetSnapshot(); snap != nil {
		s.mu.Lock()
		s.snaps = append(s.snaps, snap)
		s.at = append(s.at, time.Now())
		s.mu.Unlock()
	}
	return nil
}

func (s *snapConn) got() ([]*pb.Snapshot, []time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*pb.Snapshot(nil), s.snaps...), append([]time.Time(nil), s.at...)
}

func (s *snapConn) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.snaps)
}

// 频率和 tick 编号:3 秒内收到 60 ± 2 个快照,相邻快照的 tick 严格差 1。
// 只测 3 秒是为了 make test 不变慢;长时间的频率由压测的快照到达率来验(docs/BENCHMARK.md)。
func TestTickRateAndNumbering(t *testing.T) {
	t.Parallel()
	m := newTickManager(t)
	c := &snapConn{}
	must(m.Create(1, c))

	time.Sleep(time.Second) // 先跑一会儿,避开房间刚建好时的第一个 tick
	before := c.count()
	time.Sleep(3 * time.Second)
	n := c.count() - before
	if n < 58 || n > 62 {
		t.Errorf("got %d snapshots in 3s, want 60 ± 2", n)
	}

	snaps, _ := c.got()
	for i := 1; i < len(snaps); i++ {
		// 发送队列满时网关断开连接、不丢帧(D4),所以没有「丢帧导致跳号」的情况
		if snaps[i].GetTick() != snaps[i-1].GetTick()+1 {
			t.Fatalf("tick %d followed by %d", snaps[i-1].GetTick(), snaps[i].GetTick())
		}
	}
}

// 每个成员都收到同一份快照,快照里有所有成员,顺序是加入顺序。
func TestSnapshotHasAllMembers(t *testing.T) {
	t.Parallel()
	m := newTickManager(t)
	a, b := &snapConn{}, &snapConn{}
	r := must(m.Create(1, a))
	must(m.Join(r.ID, 2, b))
	waitFor(t, "snapshots", func() bool { return a.count() > 3 && b.count() > 3 })

	sa, _ := a.got()
	sb, _ := b.got()
	last := sb[len(sb)-1]
	ps := last.GetPlayers()
	if last.GetRoomId() != r.ID || len(ps) != 2 || ps[0].GetPlayerId() != 1 || ps[1].GetPlayerId() != 2 {
		t.Fatalf("snapshot = %v", last)
	}
	// 同一个 tick 两个人收到的内容一样
	for _, x := range sa {
		if x.GetTick() == last.GetTick() && x.String() != last.String() {
			t.Fatalf("tick %d differs:\n%v\n%v", x.GetTick(), x, last)
		}
	}
}

// 输入经过 Manager.Input 进 world,下一个快照里能看到位置变化和 last_input_seq。
func TestInputMovesPlayer(t *testing.T) {
	t.Parallel()
	m := newTickManager(t)
	c := &snapConn{}
	must(m.Create(1, c))
	waitFor(t, "first snapshot", func() bool { return c.count() > 0 })
	snaps, _ := c.got()
	x0 := snaps[0].GetPlayers()[0].GetX()

	if err := m.Input(1, sim.Input{Seq: 7, Dir: sim.Vec{X: 1}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "input applied", func() bool {
		s, _ := c.got()
		p := s[len(s)-1].GetPlayers()[0]
		return p.GetLastInputSeq() == 7 && p.GetX() > x0
	})
	if err := m.Input(99, sim.Input{Seq: 1}); err != ErrNotInRoom {
		t.Fatalf("Input for player not in room: err = %v, want ErrNotInRoom", err)
	}
}

// 一个 tick 内灌 1000 个输入:tick 不变慢,快照间隔和平时一样,位移不超过一个 tick 的量。
func TestInputFloodDoesNotSlowTick(t *testing.T) {
	t.Parallel()
	m := newTickManager(t)
	a, b := &snapConn{}, &snapConn{}
	r := must(m.Create(1, a))
	must(m.Join(r.ID, 2, b))
	waitFor(t, "snapshots", func() bool { return b.count() > 2 })

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for seq := uint32(1); ; seq++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = m.Input(1, sim.Input{Seq: seq, Dir: sim.Vec{X: 100}})
		}
	}()
	time.Sleep(time.Second)
	close(stop)
	<-done

	snaps, at := b.got()
	for i := 1; i < len(snaps); i++ {
		if gap := at[i].Sub(at[i-1]); gap > testTick*3/2 {
			t.Errorf("snapshot gap %v during input flood", gap)
		}
		// 每个 tick 最多走 Speed × dt,灌多少输入都不会更快(D16)
		dx := snaps[i].GetPlayers()[0].GetX() - snaps[i-1].GetPlayers()[0].GetX()
		if dx > float32(sim.Speed*testTick.Seconds())+1e-4 {
			t.Fatalf("player moved %v in one tick", dx)
		}
	}
}

// 建 100 个房间再全部解散,goroutine 数回到基线:每个房间的 tick goroutine 都退出了。
func TestNoTickGoroutineLeak(t *testing.T) {
	m := newTickManager(t)
	base := runtime.NumGoroutine()
	for pid := uint64(1); pid <= 100; pid++ {
		must(m.Create(pid, nopSender{}))
	}
	if got := runtime.NumGoroutine(); got < base+100 {
		t.Fatalf("goroutines = %d after creating 100 rooms, want >= %d", got, base+100)
	}
	for pid := uint64(1); pid <= 100; pid++ {
		must(m.Leave(pid))
	}
	waitFor(t, "tick goroutines to exit", func() bool { return runtime.NumGoroutine() <= base })
	if m.Count() != 0 {
		t.Fatalf("Count = %d, want 0", m.Count())
	}
}

func TestLateness(t *testing.T) {
	start := time.Unix(1000, 0)
	at := func(ms int) time.Time { return start.Add(time.Duration(ms) * time.Millisecond) }
	tests := []struct {
		fired, woke int
		want        time.Duration
	}{
		{50, 50, 0},
		{51, 52, 2 * time.Millisecond},
		{150, 153, 3 * time.Millisecond},  // 第 3 个 tick,前面的不用数
		{1049, 1049, 0},                   // 比理想时刻早一点点(时钟粒度):按 0 算
		{200, 230, 30 * time.Millisecond}, // fired 准点,goroutine 30ms 后才被调度到
	}
	for _, tt := range tests {
		if got := lateness(start, at(tt.fired), at(tt.woke), 50*time.Millisecond); got != tt.want {
			t.Errorf("lateness(fired=%d, woke=%d) = %v, want %v", tt.fired, tt.woke, got, tt.want)
		}
	}
}

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
