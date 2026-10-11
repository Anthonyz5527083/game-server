package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Anthonyz5527083/game-server/internal/room"
)

// MySQL 没有 miniredis 那样的进程内替身,这组测试连真的 MySQL:
// 设了 GS_TEST_MYSQL_DSN 才跑(比如 root:pass@tcp(127.0.0.1:3306)/game_test),没设就跳过。
// 库要事先建好、可以随便清空。测试先执行 deploy/mysql/init.sql,所以顺带验证了建表脚本能重复执行。
func openTestMySQL(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("GS_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("GS_TEST_MYSQL_DSN not set")
	}
	db, err := OpenMySQL(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	script, err := os.ReadFile("../../deploy/mysql/init.sql")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 { // 执行两遍:建表脚本必须可以重复执行
		for _, stmt := range splitSQL(string(script)) {
			if _, err := db.Exec(stmt); err != nil {
				t.Fatalf("init.sql: %v\n%s", err, stmt)
			}
		}
	}
	if _, err := db.Exec("TRUNCATE TABLE match_records"); err != nil {
		t.Fatal(err)
	}
	return db
}

// splitSQL 按行尾的分号切分语句,去掉 -- 注释。只够用来跑 init.sql,不是通用的 SQL 解析。
func splitSQL(script string) []string {
	var stmts []string
	var cur strings.Builder
	for _, line := range strings.Split(script, "\n") {
		if t := strings.TrimSpace(line); t == "" || strings.HasPrefix(t, "--") {
			continue
		}
		cur.WriteString(line + "\n")
		if strings.HasSuffix(strings.TrimSpace(line), ";") {
			stmts = append(stmts, strings.TrimSuffix(strings.TrimSpace(cur.String()), ";"))
			cur.Reset()
		}
	}
	return stmts
}

// T2.9 的存储层:种子账号查得到,不存在的查不到。
func TestAccountsExists(t *testing.T) {
	db := openTestMySQL(t)
	a := NewAccounts(db)
	ctx := context.Background()
	for pid, want := range map[uint64]bool{1: true, 500: true, 10000: true, 10001: false, 0: false} {
		got, err := a.Exists(ctx, pid)
		if err != nil || got != want {
			t.Errorf("Exists(%d) = %v, %v; want %v", pid, got, err, want)
		}
	}
}

// T4.8 的存储层:写进去的每个字段读出来都一样(时间精确到毫秒);重复写是幂等的。
func TestInsertMatch(t *testing.T) {
	db := openTestMySQL(t)
	start := time.Date(2026, 10, 11, 12, 0, 0, 123_000_000, time.UTC)
	rec := room.MatchRecord{RoomID: 7, StartedAt: start, EndedAt: start.Add(95*time.Second + 456*time.Millisecond),
		Ticks: 1909, Players: []uint64{3, 1, 2}, PeakPlayers: 3}
	// 写两次:模拟「第一次其实写进去了、应答超时、重试」,表里只能有一行。
	for range 2 {
		if err := NewMatchRecords(db).InsertMatch(context.Background(), rec); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM match_records`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("rows = %d (err %v), want 1", n, err)
	}

	var got room.MatchRecord
	var players string
	err := db.QueryRow(`SELECT room_id, started_at, ended_at, ticks, peak_players, players FROM match_records`).
		Scan(&got.RoomID, &got.StartedAt, &got.EndedAt, &got.Ticks, &got.PeakPlayers, &players)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(players), &got.Players); err != nil {
		t.Fatal(err)
	}
	if got.RoomID != rec.RoomID || !got.StartedAt.Equal(rec.StartedAt) || !got.EndedAt.Equal(rec.EndedAt) ||
		got.Ticks != rec.Ticks || got.PeakPlayers != rec.PeakPlayers || !slices.Equal(got.Players, rec.Players) {
		t.Fatalf("read back %+v, wrote %+v", got, rec)
	}
}

// MySQL 连不上时返回 error,不是「账号不存在」。
func TestAccountsUnreachable(t *testing.T) {
	db, err := OpenMySQL("u:p@tcp(127.0.0.1:1)/game")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if ok, err := NewAccounts(db).Exists(context.Background(), 1); err == nil || ok {
		t.Fatalf("Exists on unreachable db = %v, %v; want error", ok, err)
	}
}
