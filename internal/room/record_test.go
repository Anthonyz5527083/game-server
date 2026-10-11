package room

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"
)

// memSink 把记录存在内存里;failFirst 次写入返回错误。
type memSink struct {
	mu        sync.Mutex
	recs      []MatchRecord
	failFirst int
	calls     int
}

func (s *memSink) InsertMatch(_ context.Context, r MatchRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.failFirst {
		return errors.New("db down")
	}
	s.recs = append(s.recs, r)
	return nil
}

func (s *memSink) got() []MatchRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.recs)
}

func newRecordingManager(t *testing.T, sink RecordSink) (*Manager, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	m := NewManager(Config{Capacity: 8, TickInterval: testTick, RecordMatches: true,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	t.Cleanup(m.Close)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.RunRecorder(ctx, sink)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })
	return m, cancel, done
}

// 房间解散时写一条记录,每个字段都对:进过房间的人都在、按第一次进入的顺序,
// 中途离开又回来的不重复;峰值人数;tick 数和时长对得上。
func TestMatchRecordOnRoomClose(t *testing.T) {
	sink := &memSink{}
	m, _, _ := newRecordingManager(t, sink)

	before := time.Now()
	r := must(m.Create(1, nopSender{}))
	must(m.Join(r.ID, 2, nopSender{}))
	must(m.Join(r.ID, 3, nopSender{}))
	must(m.Leave(2))
	must(m.Join(r.ID, 2, nopSender{})) // 离开又回来
	time.Sleep(300 * time.Millisecond)
	for _, pid := range []uint64{1, 2, 3} {
		must(m.Leave(pid))
	}
	after := time.Now()

	waitFor(t, "match record", func() bool { return len(sink.got()) == 1 })
	rec := sink.got()[0]
	if rec.RoomID != r.ID || !slices.Equal(rec.Players, []uint64{1, 2, 3}) || rec.PeakPlayers != 3 {
		t.Fatalf("record = %+v", rec)
	}
	if rec.StartedAt.Before(before) || rec.EndedAt.After(after) || !rec.EndedAt.After(rec.StartedAt) {
		t.Fatalf("times: started %v ended %v, test ran %v..%v", rec.StartedAt, rec.EndedAt, before, after)
	}
	// 300ms 里大约 6 个 tick;tick 数不会超过经过的时间能容纳的数量
	if maxTicks := uint64(rec.EndedAt.Sub(rec.StartedAt)/testTick) + 1; rec.Ticks < 4 || rec.Ticks > maxTicks {
		t.Fatalf("ticks = %d, want 4..%d", rec.Ticks, maxTicks)
	}
}

// 写失败会重试;没开 RecordMatches 时什么都不记。
func TestMatchRecordRetriesAndDisabled(t *testing.T) {
	sink := &memSink{failFirst: 2}
	m, _, _ := newRecordingManager(t, sink)
	must(m.Create(1, nopSender{}))
	must(m.Leave(1))
	waitFor(t, "record after retries", func() bool { return len(sink.got()) == 1 })

	off := newManager() // RecordMatches: false
	must(off.Create(1, nopSender{}))
	must(off.Leave(1))
	if off.records != nil {
		t.Fatal("records queue exists although RecordMatches is false")
	}
}

// 关停:ctx 取消之后,队列里剩下的记录也会写完。
func TestRecorderDrainsOnStop(t *testing.T) {
	sink := &memSink{}
	m := NewManager(Config{Capacity: 8, RecordMatches: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	for pid := uint64(1); pid <= 10; pid++ {
		must(m.Create(pid, nopSender{}))
		must(m.Leave(pid))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 还没开始消费就取消:10 条都在队列里
	m.RunRecorder(ctx, sink)
	if n := len(sink.got()); n != 10 {
		t.Fatalf("wrote %d records, want 10", n)
	}
}

// slowSink 每次写要 100ms,并且尊重 ctx:ctx 先结束就返回 ctx.Err()。started 在第一次开始写时 close。
type slowSink struct {
	memSink
	started chan struct{}
	once    sync.Once
}

func (s *slowSink) InsertMatch(ctx context.Context, r MatchRecord) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-time.After(100 * time.Millisecond):
		return s.memSink.InsertMatch(ctx, r)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// 关停时正在写的那一条不能被打断。第一版把 RunRecorder 的 ctx 直接传给了写入,
// 关停一取消,正在写的记录就报 context canceled 丢了(本机 MySQL 实测撞到)。
func TestRecorderFinishesInflightWriteOnStop(t *testing.T) {
	sink := &slowSink{started: make(chan struct{})}
	m := NewManager(Config{Capacity: 8, RecordMatches: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	must(m.Create(1, nopSender{}))
	must(m.Leave(1))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.RunRecorder(ctx, sink)
		close(done)
	}()
	<-sink.started
	cancel()
	<-done
	if n := len(sink.got()); n != 1 {
		t.Fatalf("wrote %d records, want 1: in-flight write was cancelled", n)
	}
}
