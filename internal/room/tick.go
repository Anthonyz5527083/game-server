package room

import (
	"time"

	"github.com/Anthonyz5527083/game-server/internal/gateway"
	"github.com/Anthonyz5527083/game-server/internal/metrics"
	"github.com/Anthonyz5527083/game-server/internal/pb"
)

// runTicks 是一个房间的 tick 循环:每 interval 推进一次 world、广播一次快照,直到房间销毁。
//
// 每个房间一个 goroutine,不是全局一个 goroutine 轮流 tick 所有房间:
// 房间之间没有共享状态,各自的 tick 互不等待;一个房间 tick 慢了只影响它自己的 8 个人。
// 63 个房间就是 63 个 goroutine,每个大部分时间阻塞在 ticker 上,调度开销可以忽略。
//
// 节拍用 time.Ticker,不用 Sleep(D20):Ticker 按「上一次的预定时刻 + interval」排下一次,
// 处理耗时不会累积;「干活 + Sleep(50ms)」每一轮都多出干活的时间和唤醒延迟,本机实测 600 轮慢了 4.3%。
func (m *Manager) runTicks(r *room) {
	defer m.ticks.Done()
	t := time.NewTicker(m.interval)
	defer t.Stop()
	dt := m.interval.Seconds() // 固定步长:位置只取决于 tick 数,不受调度抖动影响(D20)
	start := time.Now()
	for {
		select {
		case <-r.stop:
			return
		case now := <-t.C:
			// 准点程度:这一次实际醒来的时刻,减去离它最近的理想时刻 start + k·interval。
			metrics.TickLateness.Observe(lateness(start, now, time.Now(), m.interval).Seconds())
			metrics.TickDuration.Observe(m.tick(r, dt).Seconds())
		}
	}
}

// lateness 返回 woke 比离它最近的理想 tick 时刻晚了多久。理想时刻是 start + k·interval;
// 用「最近的」而不是自己数第几次,Ticker 在接收方太慢时会丢 tick,自己数会把丢掉的那次算成迟到。
// fired 是 Ticker 送进 channel 的时间,只用来确定 k;woke 是 goroutine 真正开始干活的时间。
func lateness(start, fired, woke time.Time, interval time.Duration) time.Duration {
	k := (fired.Sub(start) + interval/2) / interval
	return max(0, woke.Sub(start.Add(k*interval)))
}

// tick 推进一次并广播快照,返回耗时(从开始等锁到投递给最后一个人)。
//
// 快照编码一次,所有成员共享同一个 []byte;持 room.mu 投递,SendFrame 不阻塞(D14 的同一套理由)。
// 持锁投递还保证了快照和 JOINED / LEFT 事件的顺序与房间状态一致。
func (m *Manager) tick(r *room, dt float64) time.Duration {
	begin := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.members) == 0 { // 最后一个人刚走,房间已经销毁,stop 还没被看到
		return time.Since(begin)
	}
	r.world.Advance(dt)
	env := snapshotEnvelope(r)
	frame, err := gateway.Marshal(env)
	if err != nil {
		m.log.Error("encode snapshot", "room", r.id, "err", err)
		return time.Since(begin)
	}
	sent := 0
	for _, mb := range r.members {
		// 失败说明对方的连接在关(或者队列满刚被关):它的 OnClose 会调 Leave,和 D15 一样不在这里处理。
		if mb.conn.SendFrame(frame) == nil {
			sent++
		}
	}
	metrics.Sent(env, sent)
	return time.Since(begin)
}

// snapshotEnvelope 把 world 转成协议里的快照。调用方持有 r.mu。
func snapshotEnvelope(r *room) *pb.Envelope {
	ps := r.world.Players()
	states := make([]*pb.PlayerState, len(ps))
	for i, p := range ps {
		states[i] = &pb.PlayerState{
			PlayerId:     p.ID,
			X:            float32(p.Pos.X),
			Y:            float32(p.Pos.Y),
			LastInputSeq: p.LastSeq,
		}
	}
	return &pb.Envelope{Payload: &pb.Envelope_Snapshot{Snapshot: &pb.Snapshot{
		RoomId:  r.id,
		Tick:    r.world.Tick(),
		Players: states,
	}}}
}
