package storage

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"

	"github.com/Anthonyz5527083/game-server/internal/room"
)

// RoomMirror 把内存里的房间镜像到 Redis,实现 room.Mirror:
//
//	gs:rooms          → set,所有房间号
//	gs:room:{roomID}  → set,房间里的玩家 ID
//
// 成员用 set:外部最常问的是「X 在不在这个房间」(SISMEMBER,O(1))和「房间几个人」(SCARD)。
// 加入顺序只在内存里有意义,镜像不保留。
//
// 假设只有一个游戏服进程写这些 key(D13)。多实例时 key 要带上 node。
type RoomMirror struct {
	rdb *redis.Client
}

func NewRoomMirror(rdb *redis.Client) *RoomMirror { return &RoomMirror{rdb: rdb} }

const roomsKey = "gs:rooms"

func RoomKey(id uint32) string { return "gs:room:" + strconv.FormatUint(uint64(id), 10) }

// Apply 在一个 MULTI/EXEC 里覆盖写 upserts、删除 deletes:外部读者看不到改了一半的状态。
func (m *RoomMirror) Apply(ctx context.Context, upserts []room.Info, deletes []uint32) error {
	_, err := m.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		for _, info := range upserts {
			writeRoom(ctx, p, info)
		}
		for _, id := range deletes {
			p.Del(ctx, RoomKey(id))
			p.SRem(ctx, roomsKey, id)
		}
		return nil
	})
	return err
}

// Replace 让 Redis 里只剩 all 这些房间:先读出现有的房间号,再在一个事务里删掉全部、写入新的。
// 读和写之间如果有别人改了 gs:rooms,这里会漏删;单写者的前提下不会发生。
func (m *RoomMirror) Replace(ctx context.Context, all []room.Info) error {
	old, err := m.rdb.SMembers(ctx, roomsKey).Result()
	if err != nil {
		return err
	}
	_, err = m.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		for _, id := range old {
			p.Del(ctx, "gs:room:"+id)
		}
		p.Del(ctx, roomsKey)
		for _, info := range all {
			writeRoom(ctx, p, info)
		}
		return nil
	})
	return err
}

func writeRoom(ctx context.Context, p redis.Pipeliner, info room.Info) {
	key := RoomKey(info.ID)
	p.Del(ctx, key)
	if len(info.Members) > 0 {
		members := make([]any, len(info.Members))
		for i, id := range info.Members {
			members[i] = id
		}
		p.SAdd(ctx, key, members...)
	}
	p.SAdd(ctx, roomsKey, info.ID)
}

// RoomMembers 读一个房间的成员(无序)。主要给测试和排障用。
func (m *RoomMirror) RoomMembers(ctx context.Context, id uint32) ([]uint64, error) {
	strs, err := m.rdb.SMembers(ctx, RoomKey(id)).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(strs))
	for _, s := range strs {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, err
		}
		ids = append(ids, n)
	}
	return ids, nil
}

// RoomIDs 读所有房间号(无序)。
func (m *RoomMirror) RoomIDs(ctx context.Context) ([]uint32, error) {
	strs, err := m.rdb.SMembers(ctx, roomsKey).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]uint32, 0, len(strs))
	for _, s := range strs {
		n, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return nil, err
		}
		ids = append(ids, uint32(n))
	}
	return ids, nil
}
