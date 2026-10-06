package room

import (
	"cmp"
	"errors"
	"log/slog"
	"slices"
	"sync"

	"github.com/Anthonyz5527083/game-server/internal/gateway"
	"github.com/Anthonyz5527083/game-server/internal/pb"
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

// Manager 管理所有房间。零值不能用,用 NewManager。
type Manager struct {
	capacity int
	log      *slog.Logger

	mu     sync.Mutex
	rooms  map[uint32]*room
	where  map[uint64]*room // 玩家 → 所在房间
	nextID uint32

	// 镜像同步(见 mirror.go):有变化的房间号记在 dirty 里,再往 wake 里丢一个信号。
	dirty map[uint32]struct{}
	wake  chan struct{}
}

type room struct {
	id      uint32
	members []member
}

type member struct {
	playerID uint64
	conn     Sender
}

func NewManager(capacity int, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{
		capacity: capacity,
		log:      log,
		rooms:    make(map[uint32]*room),
		where:    make(map[uint64]*room),
		dirty:    make(map[uint32]struct{}),
		wake:     make(chan struct{}, 1),
	}
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
	r.members = slices.DeleteFunc(r.members, func(mb member) bool { return mb.playerID == playerID })
	m.markDirtyLocked(r.id)
	info := m.infoLocked(r)
	if len(r.members) == 0 {
		delete(m.rooms, r.id)
		m.log.Info("room closed", "room", r.id)
		return info, nil
	}
	m.broadcastLocked(r, pb.RoomEvent_LEFT, playerID, info)
	return info, nil
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

// ---------- 以下方法名带 Locked 后缀:调用方必须已经持有 m.mu ----------

func (m *Manager) newRoomLocked() *room {
	m.nextID++
	r := &room{id: m.nextID}
	m.rooms[r.id] = r
	m.log.Info("room created", "room", r.id)
	return r
}

func (m *Manager) addLocked(r *room, playerID uint64, conn Sender) {
	r.members = append(r.members, member{playerID: playerID, conn: conn})
	m.where[playerID] = r
	m.markDirtyLocked(r.id)
	m.broadcastLocked(r, pb.RoomEvent_JOINED, playerID, m.infoLocked(r))
}

func (m *Manager) infoLocked(r *room) Info {
	ids := make([]uint64, len(r.members))
	for i, mb := range r.members {
		ids[i] = mb.playerID
	}
	return Info{ID: r.id, Capacity: m.capacity, Members: ids}
}

// broadcastLocked 把一个事件发给房间里除 actor 以外的所有人。
//
// 编码一次,所有人共享同一个 []byte(D14)。持锁发送:SendFrame 不阻塞,
// 持锁的代价是几微秒;换来的是事件顺序和房间状态的变化顺序一致。
// 如果放到锁外发,两个人同时加入时,第三个人可能先收到「C 进来了 [A B C]」
// 再收到「B 进来了 [A B]」,他看到的房间就倒退了。
func (m *Manager) broadcastLocked(r *room, kind pb.RoomEvent_Kind, actor uint64, info Info) {
	frame, err := gateway.Marshal(&pb.Envelope{Payload: &pb.Envelope_RoomEvent{RoomEvent: &pb.RoomEvent{
		Kind:     kind,
		PlayerId: actor,
		Room:     ToProto(info),
	}}})
	if err != nil {
		m.log.Error("encode room event", "room", r.id, "err", err)
		return
	}
	for _, mb := range r.members {
		if mb.playerID == actor {
			continue
		}
		// 发送失败说明对方的连接已经在关(或者因为队列满刚被关):它的 OnClose 会调 Leave,这里不用管。
		_ = mb.conn.SendFrame(frame)
	}
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
