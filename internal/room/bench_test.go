package room

import (
	"fmt"
	"sync/atomic"
	"testing"
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
