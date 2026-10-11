// Package sim 是房间里的游戏逻辑:玩家位置怎么随输入变化。
//
// 这里只有计算,不碰网络、锁和时钟:dt 由调用方传进来,所以能脱离真实时间做表驱动测试(D20)。
// 并发由调用方负责,生产里是 room 包的 tick 循环,它持有房间锁调用 World 的方法。
//
// 包名不叫 sync:和标准库重名,同时用到两个的文件就得起别名,读代码的人每次都要多想一下。
package sim

import "math"

const (
	// Speed 是移速,单位 / 秒。
	Speed = 5.0
	// ArenaSize 是场地边长,场地是 [0, ArenaSize] × [0, ArenaSize]。
	ArenaSize = 100.0
	// spawnRadius:新玩家出生在场地中心周围这个半径的圆上,按加入顺序每人错开 45°,不会叠在一起。
	spawnRadius = 10.0
)

// Vec 是二维向量。内部用 float64 计算,发快照时才转 float32。
type Vec struct{ X, Y float64 }

// Input 是一条移动输入:Dir 是客户端发来的原始方向,还没归一化。
type Input struct {
	Seq uint32
	Dir Vec
}

// Player 是一个玩家的同步状态。
type Player struct {
	ID      uint64
	Pos     Vec
	Dir     Vec    // 当前生效的方向,已经归一化
	LastSeq uint32 // 最后一个被应用的输入序号

	pending    Input // 上一个 tick 之后收到的最新输入,下一个 tick 才生效
	hasPending bool
}

// ClampDir 把方向的长度截到 1 以内。长度不到 1 的保留(手柄摇杆推一半就走一半速度)。
// NaN 和 Inf 当成 0:protobuf 的 float 能装下它们,不挡住的话一个 NaN 会让这个玩家的坐标永远是 NaN。
func ClampDir(d Vec) Vec {
	if !finite(d.X) || !finite(d.Y) {
		return Vec{}
	}
	l := math.Hypot(d.X, d.Y)
	if l <= 1 {
		return d
	}
	return Vec{d.X / l, d.Y / l}
}

// Step 是一个玩家在一个 tick 里的移动:pos 沿 dir 走 Speed × dt,再截到场地内。
// dir 必须已经过 ClampDir。纯函数:不读时钟、不改入参。
func Step(pos, dir Vec, dt float64) Vec {
	return Vec{
		X: clamp(pos.X+dir.X*Speed*dt, 0, ArenaSize),
		Y: clamp(pos.Y+dir.Y*Speed*dt, 0, ArenaSize),
	}
}

// World 是一个房间里全部玩家的状态。不是并发安全的,由调用方加锁。
type World struct {
	tick    uint64
	players []Player // 按加入顺序
	joined  int      // 一共加入过多少人,决定出生点
}

// Tick 返回已经推进了多少个 tick。
func (w *World) Tick() uint64 { return w.tick }

// Players 返回所有玩家的状态,按加入顺序。返回的是内部 slice,调用方只读、不能留着用到下一次修改之后。
func (w *World) Players() []Player { return w.players }

// Add 加入一个玩家,出生在中心周围的圆上。
func (w *World) Add(id uint64) {
	a := float64(w.joined) * math.Pi / 4
	w.joined++
	w.players = append(w.players, Player{
		ID:  id,
		Pos: Vec{ArenaSize/2 + spawnRadius*math.Cos(a), ArenaSize/2 + spawnRadius*math.Sin(a)},
	})
}

// Remove 移除一个玩家。不存在就什么都不做。
func (w *World) Remove(id uint64) {
	for i := range w.players {
		if w.players[i].ID == id {
			w.players = append(w.players[:i], w.players[i+1:]...)
			return
		}
	}
}

// SetInput 记下玩家的最新输入,下一次 Advance 时生效。玩家不在这个 world 里返回 false。
//
// 同一个 tick 里多次调用,只有最后一次生效(D16):输入表示「现在按着哪个方向」,
// 不是「走一步」。如果累加,客户端多发几条输入就能走得更快。
func (w *World) SetInput(id uint64, in Input) bool {
	for i := range w.players {
		if w.players[i].ID == id {
			w.players[i].pending, w.players[i].hasPending = in, true
			return true
		}
	}
	return false
}

// Advance 推进一个 tick:先应用每个玩家排队的输入,再按当前方向移动 dt 秒。
// 位置只在这里变:读 goroutine 收到输入只是排队,不直接改状态。
func (w *World) Advance(dt float64) {
	w.tick++
	for i := range w.players {
		p := &w.players[i]
		if p.hasPending {
			p.Dir, p.LastSeq = ClampDir(p.pending.Dir), p.pending.Seq
			p.hasPending = false
		}
		p.Pos = Step(p.Pos, p.Dir, dt)
	}
}

func finite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

func clamp(v, lo, hi float64) float64 { return max(lo, min(v, hi)) }
