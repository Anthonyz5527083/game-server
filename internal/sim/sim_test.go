package sim

import (
	"math"
	"testing"
)

const dt = 0.05 // 20Hz

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func nearVec(a, b Vec) bool { return near(a.X, b.X) && near(a.Y, b.Y) }

// Step 是纯函数:给定位置、方向和 dt,位置是确定的。
func TestStep(t *testing.T) {
	d := Speed * dt // 一个 tick 走的距离:0.25
	diag := 1 / math.Sqrt2
	tests := []struct {
		name     string
		pos, dir Vec
		dt       float64
		want     Vec
	}{
		{"静止", Vec{50, 50}, Vec{}, dt, Vec{50, 50}},
		{"向右", Vec{50, 50}, Vec{1, 0}, dt, Vec{50 + d, 50}},
		{"向左上", Vec{50, 50}, Vec{-diag, diag}, dt, Vec{50 - d*diag, 50 + d*diag}},
		{"半速", Vec{50, 50}, Vec{0, -0.5}, dt, Vec{50, 50 - d/2}},
		{"dt 为 0 不动", Vec{50, 50}, Vec{1, 0}, 0, Vec{50, 50}},
		{"一秒", Vec{10, 10}, Vec{1, 0}, 1, Vec{10 + Speed, 10}},
		{"右边界截住", Vec{ArenaSize - 0.1, 50}, Vec{1, 0}, dt, Vec{ArenaSize, 50}},
		{"左下角截住", Vec{0.1, 0.1}, Vec{-diag, -diag}, 1, Vec{0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Step(tt.pos, tt.dir, tt.dt); !nearVec(got, tt.want) {
				t.Errorf("Step(%v, %v, %v) = %v, want %v", tt.pos, tt.dir, tt.dt, got, tt.want)
			}
		})
	}
}

// 方向 (100, 100) 和 (1, 1) 走出同样的距离,都等于 Speed × dt。
func TestClampDirSameDistance(t *testing.T) {
	start := Vec{50, 50}
	for _, raw := range []Vec{{100, 100}, {1, 1}, {3, 4}, {0, -7}} {
		got := Step(start, ClampDir(raw), dt)
		if dist := math.Hypot(got.X-start.X, got.Y-start.Y); !near(dist, Speed*dt) {
			t.Errorf("dir %v: moved %v, want %v", raw, dist, Speed*dt)
		}
	}
}

func TestClampDir(t *testing.T) {
	nan, inf := math.NaN(), math.Inf(1)
	tests := []struct{ in, want Vec }{
		{Vec{0, 0}, Vec{0, 0}},
		{Vec{0.3, 0.4}, Vec{0.3, 0.4}}, // 长度 0.5,保留
		{Vec{3, 4}, Vec{0.6, 0.8}},
		{Vec{nan, 1}, Vec{0, 0}},
		{Vec{1, inf}, Vec{0, 0}},
		{Vec{-inf, -inf}, Vec{0, 0}},
	}
	for _, tt := range tests {
		if got := ClampDir(tt.in); !nearVec(got, tt.want) {
			t.Errorf("ClampDir(%v) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

// 输入只在 Advance 时生效;同一个 tick 里的多个输入只有最后一个算(D16)。
func TestWorldLastInputWins(t *testing.T) {
	var w World
	w.Add(1)
	start := w.Players()[0].Pos

	w.SetInput(1, Input{Seq: 1, Dir: Vec{-1, 0}})
	w.SetInput(1, Input{Seq: 2, Dir: Vec{0, 1}})
	if p := w.Players()[0]; p.Pos != start || p.LastSeq != 0 {
		t.Fatalf("input applied before Advance: %+v", p)
	}
	w.Advance(dt)
	p := w.Players()[0]
	if want := (Vec{start.X, start.Y + Speed*dt}); !nearVec(p.Pos, want) || p.LastSeq != 2 {
		t.Fatalf("after Advance: pos=%v seq=%d, want pos=%v seq=2", p.Pos, p.LastSeq, want)
	}
}

// 方向是持续的:没有新输入时按上一个方向继续走;发 (0, 0) 才停。
func TestWorldDirectionPersists(t *testing.T) {
	var w World
	w.Add(1)
	start := w.Players()[0].Pos
	w.SetInput(1, Input{Seq: 1, Dir: Vec{1, 0}})
	for range 20 {
		w.Advance(dt)
	}
	if got := w.Players()[0].Pos.X - start.X; !near(got, Speed) {
		t.Fatalf("20 ticks moved %v, want %v", got, Speed)
	}
	w.SetInput(1, Input{Seq: 2})
	w.Advance(dt)
	x := w.Players()[0].Pos.X
	w.Advance(dt)
	if w.Players()[0].Pos.X != x {
		t.Fatal("still moving after zero input")
	}
	if w.Tick() != 22 {
		t.Fatalf("Tick = %d, want 22", w.Tick())
	}
}

func TestWorldAddRemove(t *testing.T) {
	var w World
	for id := uint64(1); id <= 3; id++ {
		w.Add(id)
	}
	ps := w.Players()
	if ps[0].Pos == ps[1].Pos || ps[1].Pos == ps[2].Pos {
		t.Fatalf("players spawned on top of each other: %v %v %v", ps[0].Pos, ps[1].Pos, ps[2].Pos)
	}
	w.Remove(2)
	w.Remove(99) // 不存在:什么都不做
	if ps := w.Players(); len(ps) != 2 || ps[0].ID != 1 || ps[1].ID != 3 {
		t.Fatalf("after Remove: %+v", ps)
	}
	if w.SetInput(2, Input{Seq: 1}) {
		t.Fatal("SetInput on removed player returned true")
	}
}
