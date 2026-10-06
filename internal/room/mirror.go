package room

import (
	"context"
	"time"
)

// Mirror 是房间镜像的存储(生产里是 Redis,见 storage.RoomMirror)。
type Mirror interface {
	// Apply 增量同步:upserts 里的房间覆盖写,deletes 里的房间删掉。
	Apply(ctx context.Context, upserts []Info, deletes []uint32) error
	// Replace 全量同步:镜像里只留下 all 这些房间。启动时用它清掉上一个进程的残留。
	Replace(ctx context.Context, all []Info) error
}

// RunMirror 把房间的变化同步到 mirror,直到 ctx 取消。
//
// 为什么异步、为什么用 dirty 集合(D13):
//   - 进出房间是在大厅的锁里做的,不能在锁里等 Redis 的网络往返;
//   - 同一个房间短时间内变很多次(8 个人陆续进来),只需要同步最后的样子。dirty 集合天然合并,
//     镜像 goroutine 醒来时在锁里拍一张快照,然后在锁外写 Redis;
//   - 写失败了把这些房间重新标脏,过一会儿再试,Redis 恢复后自动追上;
//   - 每隔 fullSync 做一次全量覆盖,Redis 重启丢了数据也能补回来;
//   - ctx 取消时再全量同步一次。调用方应该在所有连接都关掉之后才取消 ctx,
//     这样最后一次同步写进去的是「没有房间」,Redis 里不会留下上一局的残留。
func (m *Manager) RunMirror(ctx context.Context, mirror Mirror, fullSync time.Duration) {
	const retryDelay = time.Second
	full := time.NewTicker(fullSync)
	defer full.Stop()
	needFull := true // 启动先全量:清掉上一个进程留下的房间

	for {
		if needFull {
			if err := m.syncAll(ctx, mirror); err != nil {
				m.log.Warn("room mirror: full sync failed", "err", err)
			} else {
				needFull = false
			}
		} else if err := m.syncDirty(ctx, mirror); err != nil {
			m.log.Warn("room mirror: sync failed, will retry", "err", err)
		}

		var retry <-chan time.Time
		if needFull || m.hasDirty() {
			retry = time.After(retryDelay)
		}
		select {
		case <-ctx.Done():
			final, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := m.syncAll(final, mirror); err != nil {
				m.log.Warn("room mirror: final sync failed", "err", err)
			}
			cancel()
			return
		case <-m.wake:
		case <-full.C:
			needFull = true
		case <-retry:
		}
	}
}

func (m *Manager) syncAll(ctx context.Context, mirror Mirror) error {
	m.mu.Lock()
	clear(m.dirty) // 全量快照已经包含了所有变化
	all := make([]Info, 0, len(m.rooms))
	for _, r := range m.rooms {
		all = append(all, m.infoLocked(r))
	}
	m.mu.Unlock()

	// 失败了不用重新标脏:needFull 还是 true,下一轮重新拍全量快照。
	return mirror.Replace(ctx, all)
}

func (m *Manager) syncDirty(ctx context.Context, mirror Mirror) error {
	m.mu.Lock()
	if len(m.dirty) == 0 {
		m.mu.Unlock()
		return nil
	}
	var upserts []Info
	var deletes []uint32
	for id := range m.dirty {
		if r, ok := m.rooms[id]; ok {
			upserts = append(upserts, m.infoLocked(r))
		} else {
			deletes = append(deletes, id)
		}
	}
	clear(m.dirty)
	m.mu.Unlock()

	err := mirror.Apply(ctx, upserts, deletes)
	if err != nil {
		// 重新标脏。如果这期间房间又变了,下次同步拍的是新快照,不会把旧数据写回去。
		m.mu.Lock()
		for _, info := range upserts {
			m.dirty[info.ID] = struct{}{}
		}
		for _, id := range deletes {
			m.dirty[id] = struct{}{}
		}
		m.mu.Unlock()
	}
	return err
}

func (m *Manager) hasDirty() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.dirty) > 0
}
