package room

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/Anthonyz5527083/game-server/internal/gateway"
	"github.com/Anthonyz5527083/game-server/internal/sim"
)

type nopSender struct{}

func (nopSender) SendFrame([]byte) error { return nil }

// B3.1 广播只编码一次:房间里 2 人和 8 人时,allocs/op 应该一样(字节数会随成员数涨,次数不涨)。
// 每次迭代 = 一个人加入 + 离开,触发两次广播。
func BenchmarkBroadcast(b *testing.B) {
	for _, n := range []int{2, 8} {
		b.Run(fmt.Sprintf("members=%d", n), func(b *testing.B) {
			m := newManager()
			room := must(m.Create(1, nopSender{}))
			for pid := uint64(2); pid < uint64(n); pid++ {
				must(m.Join(room.ID, pid, nopSender{}))
			}
			b.ReportAllocs()
			for b.Loop() {
				must(m.Join(room.ID, 999, nopSender{}))
				must(m.Leave(999))
			}
		})
	}
}

// B3.2 一把锁在并发下的代价:每个 goroutine 不停地快速匹配、离开。用 -cpu 1,4,20 跑,看 ns/op 怎么变。
func BenchmarkQuickMatchLeave(b *testing.B) {
	m := newManager()
	var next atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		pid := next.Add(1)
		for pb.Next() {
			must(m.QuickMatch(pid, nopSender{}))
			must(m.Leave(pid))
		}
	})
}

// B4.3 一次 tick 的耗时:8 人房,推进 world + 编码快照 + 投递 8 次(投递是空操作,只量服务端自己的开销)。
func BenchmarkTick8(b *testing.B) {
	m := newManager()
	r := must(m.Create(1, nopSender{}))
	for pid := uint64(2); pid <= 8; pid++ {
		must(m.Join(r.ID, pid, nopSender{}))
	}
	rm := m.rooms[r.ID]
	for pid := uint64(1); pid <= 8; pid++ {
		_ = m.Input(pid, sim.Input{Seq: 1000 + uint32(pid), Dir: sim.Vec{X: 1, Y: 0.5}})
	}
	b.ReportAllocs()
	for b.Loop() {
		m.tick(rm, 0.05)
	}
}

// D18 的依据:8 人全量快照编码后多大。tick 编号和 last_input_seq 取压测 30 秒左右时的量级。
func TestSnapshotSize8(t *testing.T) {
	m := newManager()
	r := must(m.Create(1, nopSender{}))
	for pid := uint64(2); pid <= 8; pid++ {
		must(m.Join(r.ID, pid, nopSender{}))
	}
	rm := m.rooms[r.ID]
	for pid := uint64(1); pid <= 8; pid++ {
		_ = m.Input(pid, sim.Input{Seq: 700, Dir: sim.Vec{X: 1, Y: 0.5}})
	}
	for range 700 {
		rm.world.Advance(0.05)
	}
	frame, err := gateway.Marshal(snapshotEnvelope(rm))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("8-player snapshot frame: %d bytes (body %d)", len(frame), len(frame)-4)
	if len(frame) > 256 {
		t.Fatalf("snapshot frame %d bytes, want <= 256", len(frame))
	}
}
