package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/Anthonyz5527083/game-server/internal/room"
)

// OpenMySQL 打开一个 MySQL 连接池。dsn 是 go-sql-driver 的格式:user:pass@tcp(host:3306)/game。
//
// sql.OpenDB 不会真的去连,第一次查询时才连;所以 MySQL 比游戏服晚起来也没关系,
// 期间登录回 UNAVAILABLE,MySQL 起来之后自动恢复(和 Redis 一样,见 D26)。
func OpenMySQL(dsn string) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse mysql dsn: %w", err)
	}
	// 超时和 Redis 一样短:数据库卡住时宁可快速失败,也不要让登录挂好几秒。
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = time.Second, 2*time.Second, 2*time.Second
	cfg.ParseTime = true // DATETIME 扫进 time.Time
	cfg.Loc = time.UTC   // 存取都按 UTC,和服务器时区无关
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	// 登录风暴时(500 个人同时登录)每人一次主键查询;16 条连接足够,也不会把 MySQL 默认的 151 个连接占满。
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	db.SetConnMaxLifetime(5 * time.Minute) // 定期换连接,MySQL 的 wait_timeout 断掉的空闲连接不会被拿来用
	return db, nil
}

// Accounts 查 accounts 表。
type Accounts struct{ db *sql.DB }

func NewAccounts(db *sql.DB) *Accounts { return &Accounts{db: db} }

// Exists 返回玩家的账号存不存在。查询出错(连不上、超时)返回 error,和「不存在」区分开:
// 前者该回 UNAVAILABLE 让客户端重试,后者该直接拒绝。
func (a *Accounts) Exists(ctx context.Context, playerID uint64) (bool, error) {
	var one int
	err := a.db.QueryRowContext(ctx, "SELECT 1 FROM accounts WHERE player_id = ?", playerID).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	}
	return true, nil
}

// MatchRecords 写 match_records 表,实现 room.RecordSink。
// 同一局((room_id, started_at) 相同)写两次只留一行,重试是幂等的。
type MatchRecords struct{ db *sql.DB }

func NewMatchRecords(db *sql.DB) *MatchRecords { return &MatchRecords{db: db} }

func (m *MatchRecords) InsertMatch(ctx context.Context, r room.MatchRecord) error {
	players, err := json.Marshal(r.Players)
	if err != nil {
		return err
	}
	_, err = m.db.ExecContext(ctx,
		// 不用 INSERT IGNORE:它会把类型溢出之类的错误也降级成警告,这里只想忽略重复键。
		`INSERT INTO match_records (room_id, started_at, ended_at, ticks, peak_players, players)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE id = id`,
		r.RoomID, r.StartedAt.UTC(), r.EndedAt.UTC(), r.Ticks, r.PeakPlayers, string(players))
	return err
}
