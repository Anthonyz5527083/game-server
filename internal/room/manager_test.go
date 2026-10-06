package room

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Anthonyz5527083/game-server/internal/gateway"
	"github.com/Anthonyz5527083/game-server/internal/pb"
)

func newManager() *Manager {
	return NewManager(8, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// fakeConn 把收到的帧解码成 RoomEvent 存起来;fail 为 true 时模拟「队列满、连接已断」。
type fakeConn struct {
	mu     sync.Mutex
	events []*pb.RoomEvent
	fail   bool
}

func (f *fakeConn) SendFrame(frame []byte) error {
	if f.fail {
		return gateway.ErrSendQueueFull
	}
	env, err := gateway.ReadEnvelope(bytes.NewReader(frame))
	if err != nil {
		panic(err)
	}
	f.mu.Lock()
	f.events = append(f.events, env.GetRoomEvent())
	f.mu.Unlock()
	return nil
}

func (f *fakeConn) got() []*pb.RoomEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.events)
}

// must 用在「这一步不该失败」的地方。Go 不允许 f(t, g()) 这种写法展开多返回值,所以没带 t,失败直接 panic。
func must(info Info, err error) Info {
	if err != nil {
		panic(err)
	}
	return info
}

// T3.1 A 建房,B 加入:A 收到「B joined」,B 自己不收事件(他从返回值里知道结果)。
func TestJoinBroadcastsToOthers(t *testing.T) {
	m := newManager()
	a, b := &fakeConn{}, &fakeConn{}
	room := must(m.Create(1, a))
	info := must(m.Join(room.ID, 2, b))

	if !slices.Equal(info.Members, []uint64{1, 2}) {
		t.Fatalf("members = %v, want [1 2]", info.Members)
	}
	ev := a.got()
	if len(ev) != 1 || ev[0].GetKind() != pb.RoomEvent_JOINED || ev[0].GetPlayerId() != 2 {
		t.Fatalf("a got %v, want one JOINED(2)", ev)
	}
	if !slices.Equal(ev[0].GetRoom().GetMembers(), []uint64{1, 2}) {
		t.Fatalf("event room = %v", ev[0].GetRoom())
	}
	if len(b.got()) != 0 {
		t.Fatalf("joiner got events: %v", b.got())
	}
}

// T3.2 第 9 个人加入被拒绝。
func TestRoomFull(t *testing.T) {
	m := newManager()
	room := must(m.Create(1, &fakeConn{}))
	for pid := uint64(2); pid <= 8; pid++ {
		must(m.Join(room.ID, pid, &fakeConn{}))
	}
	if _, err := m.Join(room.ID, 9, &fakeConn{}); !errors.Is(err, ErrRoomFull) {
		t.Fatalf("err = %v, want ErrRoomFull", err)
	}
}

// T3.3 已经在房间里的人不能再建房 / 加入 / 快速匹配。
func TestOneRoomPerPlayer(t *testing.T) {
	m := newManager()
	room := must(m.Create(1, &fakeConn{}))
	other := must(m.Create(2, &fakeConn{}))
	if _, err := m.Join(other.ID, 1, &fakeConn{}); !errors.Is(err, ErrAlreadyInRoom) {
		t.Fatalf("Join: err = %v", err)
	}
	if _, err := m.Create(1, &fakeConn{}); !errors.Is(err, ErrAlreadyInRoom) {
		t.Fatalf("Create: err = %v", err)
	}
	if _, err := m.QuickMatch(1, &fakeConn{}); !errors.Is(err, ErrAlreadyInRoom) {
		t.Fatalf("QuickMatch: err = %v", err)
	}
	if id, _ := m.RoomOf(1); id != room.ID {
		t.Fatalf("RoomOf(1) = %d, want %d", id, room.ID)
	}
}

func TestJoinMissingRoom(t *testing.T) {
	if _, err := newManager().Join(42, 1, &fakeConn{}); !errors.Is(err, ErrRoomNotFound) {
		t.Fatalf("err = %v, want ErrRoomNotFound", err)
	}
}

// T3.4(房间层):离开时剩下的人收到 LEFT;最后一个人走了,房间销毁。
func TestLeaveBroadcastsAndDestroys(t *testing.T) {
	m := newManager()
	a, b := &fakeConn{}, &fakeConn{}
	room := must(m.Create(1, a))
	must(m.Join(room.ID, 2, b))

	info := must(m.Leave(2))
	if !slices.Equal(info.Members, []uint64{1}) {
		t.Fatalf("after leave: members = %v", info.Members)
	}
	ev := a.got()
	if last := ev[len(ev)-1]; last.GetKind() != pb.RoomEvent_LEFT || last.GetPlayerId() != 2 {
		t.Fatalf("a's last event = %v, want LEFT(2)", last)
	}

	must(m.Leave(1))
	if len(m.Rooms()) != 0 {
		t.Fatalf("rooms = %v, want none", m.Rooms())
	}
	if _, err := m.Join(room.ID, 3, &fakeConn{}); !errors.Is(err, ErrRoomNotFound) {
		t.Fatalf("join destroyed room: err = %v", err)
	}
	if _, err := m.Leave(1); !errors.Is(err, ErrNotInRoom) {
		t.Fatalf("leave twice: err = %v", err)
	}
}

// T3.5 抢位:房间里 1 人、空 7 个位,64 个 goroutine 同时 Join,恰好 7 个成功。
// 循环 20 轮,配合 -race 跑。
func TestConcurrentJoinExactlyFillsRoom(t *testing.T) {
	for round := range 20 {
		m := newManager()
		room := must(m.Create(1, &fakeConn{}))
		var ok, full atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range 64 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start // 让 64 个 goroutine 尽量同时开抢
				_, err := m.Join(room.ID, uint64(100+i), &fakeConn{})
				switch {
				case err == nil:
					ok.Add(1)
				case errors.Is(err, ErrRoomFull):
					full.Add(1)
				default:
					t.Errorf("unexpected err: %v", err)
				}
			}()
		}
		close(start)
		wg.Wait()
		if ok.Load() != 7 || full.Load() != 57 {
			t.Fatalf("round %d: ok=%d full=%d, want 7 / 57", round, ok.Load(), full.Load())
		}
		if n := len(m.Rooms()[0].Members); n != 8 {
			t.Fatalf("round %d: room has %d members", round, n)
		}
	}
}

// T3.6 快速匹配:20 个人依次进来,房间人数是 8 / 8 / 4。
func TestQuickMatchFillsLowestRoomFirst(t *testing.T) {
	m := newManager()
	for pid := uint64(1); pid <= 20; pid++ {
		must(m.QuickMatch(pid, &fakeConn{}))
	}
	var sizes []int
	for _, r := range m.Rooms() {
		sizes = append(sizes, len(r.Members))
	}
	if !slices.Equal(sizes, []int{8, 8, 4}) {
		t.Fatalf("room sizes = %v, want [8 8 4]", sizes)
	}
	// 第一个房间走了一个人,下一个快速匹配的人应该补进第一个房间,而不是第三个。
	must(m.Leave(3))
	info := must(m.QuickMatch(21, &fakeConn{}))
	if info.ID != m.Rooms()[0].ID {
		t.Fatalf("player 21 went to room %d, want the lowest non-full room %d", info.ID, m.Rooms()[0].ID)
	}
}

// T3.7 慢客户端隔离:一个成员发不出去(队列满),其他人照常收到广播,调用方也不受影响。
func TestBroadcastSkipsFailingMember(t *testing.T) {
	m := newManager()
	slow := &fakeConn{fail: true}
	others := []*fakeConn{{}, {}, {}}
	room := must(m.Create(1, slow))
	for i, c := range others {
		must(m.Join(room.ID, uint64(2+i), c))
	}
	must(m.Join(room.ID, 9, &fakeConn{}))
	for i, c := range others {
		ev := c.got()
		if last := ev[len(ev)-1]; last.GetPlayerId() != 9 {
			t.Fatalf("member %d did not get JOINED(9): %v", i, ev)
		}
	}
}

// 事件顺序和房间变化顺序一致:每个事件里的成员数,都比上一个事件多 1 或少 1。
func TestEventsAreOrdered(t *testing.T) {
	m := newManager()
	watcher := &fakeConn{}
	room := must(m.Create(1, watcher))
	var wg sync.WaitGroup
	for i := range 7 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pid := uint64(10 + i)
			for range 50 {
				if _, err := m.Join(room.ID, pid, &fakeConn{}); err == nil {
					_, _ = m.Leave(pid)
				}
			}
		}()
	}
	wg.Wait()
	prev := 1
	for i, ev := range watcher.got() {
		n := len(ev.GetRoom().GetMembers())
		if n != prev+1 && n != prev-1 {
			t.Fatalf("event %d: room went from %d to %d members", i, prev, n)
		}
		prev = n
	}
}

// ---------- 镜像 ----------

// memMirror 是内存里的假镜像;fail 为 true 时模拟 Redis 挂了。
type memMirror struct {
	mu    sync.Mutex
	rooms map[uint32][]uint64
	fail  atomic.Bool
}

func (mm *memMirror) Apply(_ context.Context, upserts []Info, deletes []uint32) error {
	if mm.fail.Load() {
		return errors.New("mirror down")
	}
	mm.mu.Lock()
	defer mm.mu.Unlock()
	for _, info := range upserts {
		mm.rooms[info.ID] = info.Members
	}
	for _, id := range deletes {
		delete(mm.rooms, id)
	}
	return nil
}

func (mm *memMirror) Replace(_ context.Context, all []Info) error {
	if mm.fail.Load() {
		return errors.New("mirror down")
	}
	mm.mu.Lock()
	defer mm.mu.Unlock()
	clear(mm.rooms)
	for _, info := range all {
		mm.rooms[info.ID] = info.Members
	}
	return nil
}

func (mm *memMirror) equals(rooms []Info) bool {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	want := make(map[uint32][]uint64, len(rooms))
	for _, r := range rooms {
		want[r.ID] = r.Members
	}
	return maps.EqualFunc(mm.rooms, want, slices.Equal[[]uint64])
}

func waitConverged(t *testing.T, m *Manager, mm *memMirror) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !mm.equals(m.Rooms()) {
		if time.Now().After(deadline) {
			t.Fatalf("mirror did not converge:\n memory %v\n mirror %v", m.Rooms(), mm.rooms)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func startMirror(t *testing.T, m *Manager, mm *memMirror) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.RunMirror(ctx, mm, time.Hour)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })
}

// T3.8 随机进出 1000 次之后,镜像和内存完全一致;启动时清掉上一个进程留下的房间。
func TestMirrorConverges(t *testing.T) {
	m := newManager()
	mm := &memMirror{rooms: map[uint32][]uint64{99: {7, 8}}} // 上一个进程的残留
	startMirror(t, m, mm)
	waitConverged(t, m, mm)

	rng := rand.New(rand.NewPCG(1, 2))
	for range 1000 {
		pid := uint64(rng.IntN(30) + 1)
		switch rng.IntN(4) {
		case 0:
			_, _ = m.Create(pid, &fakeConn{})
		case 1:
			_, _ = m.QuickMatch(pid, &fakeConn{})
		case 2:
			if rooms := m.Rooms(); len(rooms) > 0 {
				_, _ = m.Join(rooms[rng.IntN(len(rooms))].ID, pid, &fakeConn{})
			}
		case 3:
			_, _ = m.Leave(pid)
		}
	}
	waitConverged(t, m, mm)
}

// 镜像写失败时不丢变化:恢复之后自动追上。
func TestMirrorRetriesAfterFailure(t *testing.T) {
	m := newManager()
	mm := &memMirror{rooms: map[uint32][]uint64{}}
	startMirror(t, m, mm)
	waitConverged(t, m, mm)

	mm.fail.Store(true)
	must(m.Create(1, &fakeConn{}))
	must(m.QuickMatch(2, &fakeConn{}))
	time.Sleep(50 * time.Millisecond) // 让同步至少失败一次
	mm.fail.Store(false)
	waitConverged(t, m, mm)
}

// 停止时最后同步一次:大家都走了之后取消 ctx,镜像里不留房间。
func TestMirrorFinalSyncOnStop(t *testing.T) {
	m := newManager()
	mm := &memMirror{rooms: map[uint32][]uint64{}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.RunMirror(ctx, mm, time.Hour)
		close(done)
	}()
	must(m.QuickMatch(1, &fakeConn{}))
	waitConverged(t, m, mm)

	mm.fail.Store(true) // 让离开之后的增量同步失败,只能靠最后那一次
	must(m.Leave(1))
	mm.fail.Store(false)
	cancel()
	<-done
	if !mm.equals(nil) {
		t.Fatalf("mirror after stop = %v, want empty", mm.rooms)
	}
}
