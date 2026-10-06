package storage

import (
	"context"
	"slices"
	"testing"

	"github.com/Anthonyz5527083/game-server/internal/room"
)

func sorted[T uint32 | uint64](s []T) []T {
	slices.Sort(s)
	return s
}

func TestRoomMirrorApply(t *testing.T) {
	_, rdb := newTestRedis(t)
	m := NewRoomMirror(rdb)
	ctx := context.Background()

	err := m.Apply(ctx, []room.Info{
		{ID: 1, Members: []uint64{10, 11}},
		{ID: 2, Members: []uint64{20}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// 覆盖写:房间 1 的成员变成 [11 12],10 不能残留。
	if err := m.Apply(ctx, []room.Info{{ID: 1, Members: []uint64{11, 12}}}, []uint32{2}); err != nil {
		t.Fatal(err)
	}

	ids, err := m.RoomIDs(ctx)
	if err != nil || !slices.Equal(sorted(ids), []uint32{1}) {
		t.Fatalf("RoomIDs = %v, %v; want [1]", ids, err)
	}
	members, err := m.RoomMembers(ctx, 1)
	if err != nil || !slices.Equal(sorted(members), []uint64{11, 12}) {
		t.Fatalf("room 1 = %v, %v; want [11 12]", members, err)
	}
	if members, _ := m.RoomMembers(ctx, 2); len(members) != 0 {
		t.Fatalf("room 2 still has %v", members)
	}
}

// Replace 清掉不在新列表里的房间(上一个进程的残留)。
func TestRoomMirrorReplace(t *testing.T) {
	mr, rdb := newTestRedis(t)
	m := NewRoomMirror(rdb)
	ctx := context.Background()
	if err := m.Apply(ctx, []room.Info{{ID: 7, Members: []uint64{1}}, {ID: 8, Members: []uint64{2}}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.Replace(ctx, []room.Info{{ID: 1, Members: []uint64{3}}}); err != nil {
		t.Fatal(err)
	}
	ids, _ := m.RoomIDs(ctx)
	if !slices.Equal(ids, []uint32{1}) {
		t.Fatalf("RoomIDs = %v, want [1]", ids)
	}
	if mr.Exists(RoomKey(7)) || mr.Exists(RoomKey(8)) {
		t.Fatal("stale room keys left behind")
	}
	// 空列表:全部清掉。
	if err := m.Replace(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if keys := mr.Keys(); len(keys) != 0 {
		t.Fatalf("keys left: %v", keys)
	}
}
