// gameserver 是服务端入口:解析 flag、配置结构化日志、连 Redis、启动 TCP 网关、大厅和房间管理,
// 收到 SIGINT / SIGTERM 后关闭全部连接(并清理会话)再退出。
//
// token 密钥从环境变量 GS_TOKEN_SECRET 读,不走 flag:flag 会出现在 ps 输出和 shell 历史里。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Anthonyz5527083/game-server/internal/auth"
	"github.com/Anthonyz5527083/game-server/internal/gateway"
	"github.com/Anthonyz5527083/game-server/internal/lobby"
	"github.com/Anthonyz5527083/game-server/internal/room"
	"github.com/Anthonyz5527083/game-server/internal/storage"
)

// roomCapacity 是房间人数上限(scope:2–8 人)。
const roomCapacity = 8

func main() {
	addr := flag.String("addr", ":7777", "TCP 监听地址")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "Redis 地址")
	idle := flag.Duration("idle-timeout", 30*time.Second, "多久没收到任何帧就踢掉连接;0 表示不踢")
	loginTimeout := flag.Duration("login-timeout", 10*time.Second, "连上之后多久之内必须登录")
	sessionTTL := flag.Duration("session-ttl", time.Minute, "Redis 会话的 TTL,每 TTL/3 续期一次")
	node := flag.String("node", defaultNode(), "本进程标识,写进会话的 owner")
	logJSON := flag.Bool("log-json", false, "日志输出 JSON(默认是给人看的 text 格式)")
	logLevel := flag.String("log-level", "info", "日志级别:debug / info / warn / error")
	flag.Parse()

	logger := newLogger(*logJSON, *logLevel)
	slog.SetDefault(logger)

	if err := run(logger, *addr, *redisAddr, *idle, *loginTimeout, *sessionTTL, *node); err != nil {
		logger.Error("gameserver exited with error", "err", err)
		os.Exit(1)
	}
	logger.Info("gameserver stopped")
}

func run(logger *slog.Logger, addr, redisAddr string, idle, loginTimeout, sessionTTL time.Duration, node string) error {
	signer, err := auth.NewSigner([]byte(os.Getenv("GS_TOKEN_SECRET")))
	if err != nil {
		return fmt.Errorf("GS_TOKEN_SECRET: %w", err)
	}

	rdb := storage.NewRedis(redisAddr)
	defer rdb.Close()
	// 启动时 Redis 不通不退出:登录会回 UNAVAILABLE,Redis 起来之后自动恢复。
	// 这样 docker compose 里谁先起来都行,也不会因为 Redis 重启把游戏服一起带走。
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		logger.Warn("redis not reachable yet, logins will fail until it is", "addr", redisAddr, "err", err)
	}
	cancel()

	rooms := room.NewManager(roomCapacity, logger)
	lb := lobby.New(lobby.Config{
		Signer:       signer,
		Sessions:     storage.NewSessions(rdb, sessionTTL),
		Rooms:        rooms,
		Node:         node,
		LoginTimeout: loginTimeout,
		SessionTTL:   sessionTTL,
		StoreTimeout: time.Second,
		Logger:       logger,
	})
	srv := &gateway.Server{
		Addr:         addr,
		IdleTimeout:  idle,
		WriteTimeout: 5 * time.Second,
		Handler:      lb,
		Logger:       logger,
	}

	// Ctrl-C 和 docker stop(SIGTERM)都走这里:取消 ctx → 网关关 listener 和所有连接 →
	// 每条连接的 OnClose 删掉自己的会话 → Serve 返回。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 后台任务用单独的 ctx,等网关关完(所有 OnClose 跑完、房间都空了)才停:
	// 房间镜像停下前最后同步一次,Redis 里就不会留下这一局的房间。
	bgCtx, bgCancel := context.WithCancel(context.Background())
	var bg sync.WaitGroup
	bg.Go(func() { lb.Run(bgCtx) })                                                      // 会话续期
	bg.Go(func() { rooms.RunMirror(bgCtx, storage.NewRoomMirror(rdb), 30*time.Second) }) // 房间镜像
	err = srv.ListenAndServe(ctx)
	bgCancel()
	bg.Wait()
	return err
}

func defaultNode() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// newLogger 用标准库 log/slog:结构化、零依赖。JSON 给日志采集系统,text 给人看。
func newLogger(json bool, level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	if json {
		return slog.New(slog.NewJSONHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, opts))
}
