package room

import (
	"cmp"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/Anthonyz5527083/game-server/internal/gateway"
	"github.com/Anthonyz5527083/game-server/internal/metrics"
	"github.com/Anthonyz5527083/game-server/internal/pb"
	"github.com/Anthonyz5527083/game-server/internal/sim"
)

var (
	ErrRoomNotFound  = errors.New("room: not found")
	ErrRoomFull      = errors.New("room: full")
	ErrAlreadyInRoom = errors.New("room: player is already in a room")
	ErrNotInRoom     = errors.New("room: player is not in a room")
)

// Sender 是房间对一个成员连接的全部要求,生产里是 *gateway.Conn。
//
// SendFrame 必须不阻塞(gateway.Conn 是往有界队列投递,满了就断开对方)。
// Manager 持锁广播,就是建立在这个前提上的(D14)。
type Sender interface {
	SendFrame(frame []byte) error
}

// Info 是房间在某一时刻的快照。Members 是拷贝,调用方可以随便改。
type Info struct {
	ID       uint32
	Capacity int
	Members  []uint64 // 玩家 ID,按加入顺序
}

// Config 配置 Manager。
type Config struct {
	Capacity int // 房间人数上限
	// TickInterval 是每个房间的 tick 间隔,20Hz 就是 50ms。
	// 0 表示不起 tick 循环:只测成员关系和事件广播的单元测试用。
	TickInterval time.Duration
	// RecordMatches:房间解散时生成一条对局记录(MatchRecord),由 RunRecorder 写出去。
	// 不开的话(没有配 MySQL)什么都不记。
	RecordMatches bool
	Logger        *slog.Logger // nil 时用 slog.Default()
}

// Manager 管理所有房间。零值不能用,用 NewManager。
//
// 两层锁(D12b,加 tick 时把 D12 的一把锁拆成了两层):
//   - Manager.mu 管成员关系:房间表、玩家 → 房间索引、每个房间的成员列表;
//   - room.mu 管一个房间的游戏状态(world)。tick 循环只拿自己房间的这把锁,不碰全局锁,
//     63 个房间的 tick 互不排队,进出房间也不会被某个房间的 tick 卡住。
//
// 锁顺序:Manager.mu → room.mu → 网关。room.members 的写要同时持有两把锁,读持有任意一把就够:
// 进出房间(持 Manager.mu)和 tick(持 room.mu)都能安全地读成员列表。
type Manager struct {
	capacity int
	interval time.Duration
	log      *slog.Logger

	mu     sync.Mutex
	rooms  map[uint32]*room
	where  map[uint64]*room // 玩家 → 所在房间
	nextID uint32

	// 镜像同步(见 mirror.go):有变化的房间号记在 dirty 里,再往 wake 里丢一个信号。
	dirty map[uint32]struct{}
	wake  chan struct{}

	ticks   sync.WaitGroup   // 所有房间的 tick goroutine
	records chan MatchRecord // 等待写出的对局记录,见 record.go;没开 RecordMatches 时为 nil
}

type room struct {
	id   uint32
	stop chan struct{} // 房间销毁时 close,tick goroutine 看到就退出;不起 tick 时为 nil

	// 对局记录用,由 Manager.mu 保护
	created time.Time
	players []uint64 // 这一局进过房间的所有玩家,按第一次进入的顺序
	peak    int

	mu      sync.Mutex
	members []member  // 写:同时持有 Manager.mu 和 room.mu;读:持有任意一把
	world   sim.World // 由 room.mu 保护
}

type member struct {
	playerID uint64
	conn     Sender
}

func NewManager(cfg Config) *Manager {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	m := &Manager{
		capacity: cfg.Capacity,
		interval: cfg.TickInterval,
		log:      log,
		rooms:    make(map[uint32]*room),
		where:    make(map[uint64]*room),
		dirty:    make(map[uint32]struct{}),
		wake:     make(chan struct{}, 1),
	}
	if cfg.RecordMatches {
		m.records = make(chan MatchRecord, recordQueueLen)
	}
	return m
}

// Create 新建一个房间,playerID 成为第一个成员。
func (m *Manager) Create(playerID uint64, conn Sender) (Info, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.where[playerID]; ok {
		return Info{}, ErrAlreadyInRoom
	}
	r := m.newRoomLocked()
	m.addLocked(r, playerID, conn)
	return m.infoLocked(r), nil
}

// Join 加入指定房间。房间里的其他人收到 JOINED。
func (m *Manager) Join(roomID uint32, playerID uint64, conn Sender) (Info, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.where[playerID]; ok {
		return Info{}, ErrAlreadyInRoom
	}
	r, ok := m.rooms[roomID]
	if !ok {
		return Info{}, ErrRoomNotFound
	}
	// 「检查容量」和「加入」在同一把锁里:不会出现两个人都看到还剩 1 个位置、都进去了。
	if len(r.members) >= m.capacity {
		return Info{}, ErrRoomFull
	}
	m.addLocked(r, playerID, conn)
	return m.infoLocked(r), nil
}

// QuickMatch 进房间号最小的未满房间;都满了(或者一个房间都没有)就新建一个。
// 按房间号从小往大填,是为了让玩家尽量集中,少开半空的房间。
func (m *Manager) QuickMatch(playerID uint64, conn Sender) (Info, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.where[playerID]; ok {
		return Info{}, ErrAlreadyInRoom
	}
	var best *room
	for _, r := range m.rooms { // O(房间数);几百个房间以内比维护有序结构简单
		if len(r.members) < m.capacity && (best == nil || r.id < best.id) {
			best = r
		}
	}
	if best == nil {
		best = m.newRoomLocked()
	}
	m.addLocked(best, playerID, conn)
	return m.infoLocked(best), nil
}

// Leave 让玩家离开所在房间,返回离开之后的房间。剩下的人收到 LEFT;最后一个人走了房间就销毁。
// 断线时大厅也调它,所以它必须对「玩家不在任何房间」返回 ErrNotInRoom,而不是 panic。
func (m *Manager) Leave(playerID uint64) (Info, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.where[playerID]
	if !ok {
		return Info{}, ErrNotInRoom
	}
	delete(m.where, playerID)
	m.markDirtyLocked(r.id)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.members = slices.DeleteFunc(r.members, func(mb member) bool { return mb.playerID == playerID })
	r.world.Remove(playerID)
	info := m.infoLocked(r)
	if len(r.members) == 0 {
		delete(m.rooms, r.id)
		if r.stop != nil {
			close(r.stop) // tick goroutine 下一次 select 就退出;它这时如果正在等 r.mu,拿到锁后看到没有成员,什么都不发
		}
		m.log.Info("room closed", "room", r.id, "ticks", r.world.Tick())
		m.recordLocked(r)
		return info, nil
	}
	m.broadcastLocked(r, pb.RoomEvent_LEFT, playerID, info)
	return info, nil
}

// Input 把玩家的移动输入交给他所在房间,下一个 tick 生效。玩家不在房间里返回 ErrNotInRoom。
//
// 全局锁只用来查「玩家在哪个房间」,放掉之后才去拿房间锁:房间锁可能正被 tick 持有,
// 持着全局锁等它,会让所有房间的进出和输入都排在这一个房间的 tick 后面。
// 放锁的空档里玩家如果离开了房间,SetInput 在 world 里找不到他,这条输入就作废,不会写错房间。
func (m *Manager) Input(playerID uint64, in sim.Input) error {
	m.mu.Lock()
	r, ok := m.where[playerID]
	m.mu.Unlock()
	if !ok {
		return ErrNotInRoom
	}
	r.mu.Lock()
	ok = r.world.SetInput(playerID, in)
	r.mu.Unlock()
	if !ok {
		return ErrNotInRoom
	}
	return nil
}

// Count 返回当前房间数。给 metrics 用,不像 Rooms 那样拷贝成员列表。
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rooms)
}

// Close 停掉所有还没销毁的房间的 tick,并等这些 goroutine 退出。之后不能再用这个 Manager。
// 正常关停时房间早就随着最后一个人断线销毁了,这里只是兜底,保证进程退出前没有 tick 还在发。
func (m *Manager) Close() {
	m.mu.Lock()
	for id, r := range m.rooms {
		if r.stop != nil {
			close(r.stop)
		}
		delete(m.rooms, id)
	}
	m.mu.Unlock()
	m.ticks.Wait()
}

// RoomOf 返回玩家所在的房间号。
func (m *Manager) RoomOf(playerID uint64) (uint32, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.where[playerID]
	if !ok {
		return 0, false
	}
	return r.id, true
}

// Rooms 返回所有房间的快照,按房间号排序。
func (m *Manager) Rooms() []Info {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Info, 0, len(m.rooms))
	for _, r := range m.rooms {
		out = append(out, m.infoLocked(r))
	}
	slices.SortFunc(out, func(a, b Info) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// ---------- 以下方法名带 Locked 后缀:调用方必须已经持有 m.mu(broadcastLocked 还要持有 r.mu) ----------

func (m *Manager) newRoomLocked() *room {
	m.nextID++
	r := &room{id: m.nextID, created: time.Now()}
	m.rooms[r.id] = r
	if m.interval > 0 {
		r.stop = make(chan struct{})
		m.ticks.Add(1)
		go m.runTicks(r)
	}
	m.log.Info("room created", "room", r.id)
	return r
}

// addLocked 把玩家加进房间。成员列表、world 和 JOINED 事件在同一段 room.mu 里完成:
// 房间里的人一定先收到「X 进来了」,再收到带着 X 的快照。
func (m *Manager) addLocked(r *room, playerID uint64, conn Sender) {
	m.where[playerID] = r
	m.markDirtyLocked(r.id)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.members = append(r.members, member{playerID: playerID, conn: conn})
	r.world.Add(playerID)
	if !slices.Contains(r.players, playerID) {
		r.players = append(r.players, playerID)
	}
	r.peak = max(r.peak, len(r.members))
	m.broadcastLocked(r, pb.RoomEvent_JOINED, playerID, m.infoLocked(r))
}

func (m *Manager) infoLocked(r *room) Info {
	ids := make([]uint64, len(r.members))
	for i, mb := range r.members {
		ids[i] = mb.playerID
	}
	return Info{ID: r.id, Capacity: m.capacity, Members: ids}
}

// broadcastLocked 把一个事件发给房间里除 actor 以外的所有人。调用方持有 m.mu 和 r.mu。
//
// 编码一次,所有人共享同一个 []byte(D14)。持锁发送:SendFrame 不阻塞,
// 持锁的代价是几微秒;换来的是事件顺序和房间状态的变化顺序一致。
// 如果放到锁外发,两个人同时加入时,第三个人可能先收到「C 进来了 [A B C]」
// 再收到「B 进来了 [A B]」,他看到的房间就倒退了。
func (m *Manager) broadcastLocked(r *room, kind pb.RoomEvent_Kind, actor uint64, info Info) {
	env := &pb.Envelope{Payload: &pb.Envelope_RoomEvent{RoomEvent: &pb.RoomEvent{
		Kind:     kind,
		PlayerId: actor,
		Room:     ToProto(info),
	}}}
	frame, err := gateway.Marshal(env)
	if err != nil {
		m.log.Error("encode room event", "room", r.id, "err", err)
		return
	}
	sent := 0
	for _, mb := range r.members {
		if mb.playerID == actor {
			continue
		}
		// 发送失败说明对方的连接已经在关(或者因为队列满刚被关):它的 OnClose 会调 Leave,这里不用管。
		if mb.conn.SendFrame(frame) == nil {
			sent++
		}
	}
	metrics.Sent(env, sent)
}

func (m *Manager) markDirtyLocked(id uint32) {
	m.dirty[id] = struct{}{}
	select {
	case m.wake <- struct{}{}:
	default: // 已经有一个信号在等着了,镜像 goroutine 醒来会把所有 dirty 一起处理
	}
}

// ToProto 把 Info 转成协议里的 RoomInfo。
func ToProto(info Info) *pb.RoomInfo {
	return &pb.RoomInfo{RoomId: info.ID, Capacity: uint32(info.Capacity), Members: info.Members}
}
