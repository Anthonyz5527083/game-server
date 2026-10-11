package room

import (
	"context"
	"slices"
	"time"
)

// MatchRecord 是一局的战绩。一局 = 一个房间从建立到解散(最后一个人离开)。
type MatchRecord struct {
	RoomID      uint32
	StartedAt   time.Time
	EndedAt     time.Time
	Ticks       uint64   // 这一局推进了多少个 tick
	Players     []uint64 // 进过这个房间的所有玩家,按第一次进入的顺序
	PeakPlayers int      // 同时在房间里的最多人数
}

// RecordSink 是对局记录的存储(生产里是 MySQL,见 storage.MatchRecords)。
type RecordSink interface {
	InsertMatch(ctx context.Context, r MatchRecord) error
}

// recordQueueLen:来不及写的记录最多排这么多条。500 人的负载下一秒解散的房间是个位数。
const recordQueueLen = 1024

// recordLocked 在房间解散时生成记录并排队。调用方持有 m.mu 和 r.mu,所以只能做不阻塞的投递:
// 队列满了(MySQL 长时间写不进去)就丢掉这一条并打日志。战绩少一条不影响游戏,
// 但如果在这里等,所有进出房间都会卡在一次数据库写上。
func (m *Manager) recordLocked(r *room) {
	if m.records == nil {
		return
	}
	rec := MatchRecord{
		RoomID:      r.id,
		StartedAt:   r.created,
		EndedAt:     time.Now(),
		Ticks:       r.world.Tick(),
		Players:     slices.Clone(r.players),
		PeakPlayers: r.peak,
	}
	select {
	case m.records <- rec:
	default:
		m.log.Warn("match record dropped: queue full", "room", r.id)
	}
}

// RunRecorder 把对局记录写进 sink,直到 ctx 取消。取消之后把队列里剩下的写完再返回(最多 5 秒):
// 调用方应该在所有连接都关掉之后再取消,这时最后一批房间刚解散,它们的记录都在队列里。
//
// 写失败重试 3 次,还失败就打日志丢掉,不让一条坏记录堵住后面的。重试可能重复写同一条
// (上一次其实写进去了,只是应答超时),所以 sink 必须按 (RoomID, StartedAt) 去重。
func (m *Manager) RunRecorder(ctx context.Context, sink RecordSink) {
	for {
		select {
		case rec := <-m.records:
			// 写入用独立的 context:ctx 取消只表示「该收尾了」,不能把正在写的这一条打断。
			// 第一版直接用 ctx,关停时正在写的那条被取消,日志报丢失,其实数据库已经写进去了。
			m.writeRecord(context.Background(), sink, rec)
		case <-ctx.Done():
			drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for {
				select {
				case rec := <-m.records:
					m.writeRecord(drain, sink, rec)
				default:
					return
				}
			}
		}
	}
}

func (m *Manager) writeRecord(ctx context.Context, sink RecordSink, rec MatchRecord) {
	const attempts = 3
	for i := 1; ; i++ {
		wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := sink.InsertMatch(wctx, rec)
		cancel()
		if err == nil {
			return
		}
		if i == attempts || ctx.Err() != nil {
			m.log.Error("match record lost", "room", rec.RoomID, "players", rec.Players, "err", err)
			return
		}
		m.log.Warn("write match record failed, retrying", "room", rec.RoomID, "attempt", i, "err", err)
		select {
		case <-time.After(time.Duration(i) * 200 * time.Millisecond):
		case <-ctx.Done():
		}
	}
}
