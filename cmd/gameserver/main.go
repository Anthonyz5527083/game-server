// gameserver 是服务端入口:解析 flag、配置结构化日志、连 Redis、启动 TCP 网关、大厅和房间管理,
// 另起一个 admin HTTP 端口挂 /metrics 和 pprof。收到 SIGINT / SIGTERM 后关闭全部连接(并清理会话)再退出。
//
// token 密钥从环境变量 GS_TOKEN_SECRET 读,不走 flag:flag 会出现在 ps 输出和 shell 历史里。
// MySQL 的 DSN 带密码,同理走环境变量 GS_MYSQL_DSN;不设就不连 MySQL(不查账号表、不记战绩)。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Anthonyz5527083/game-server/internal/auth"
	"github.com/Anthonyz5527083/game-server/internal/gateway"
	"github.com/Anthonyz5527083/game-server/internal/lobby"
	"github.com/Anthonyz5527083/game-server/internal/metrics"
	"github.com/Anthonyz5527083/game-server/internal/room"
	"github.com/Anthonyz5527083/game-server/internal/storage"
)

const (
	roomCapacity = 8                     // 房间人数上限(scope:2–8 人)
	tickInterval = 50 * time.Millisecond // 20Hz
)

type config struct {
	addr, redisAddr, adminAddr, node string
	idle, loginTimeout, sessionTTL   time.Duration
}

func main() {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", ":7777", "TCP 监听地址")
	flag.StringVar(&cfg.redisAddr, "redis", "127.0.0.1:6379", "Redis 地址")
	flag.StringVar(&cfg.adminAddr, "admin", "127.0.0.1:9100", "admin HTTP 地址(/metrics、/debug/pprof/);空字符串表示不开。不要对外暴露")
	flag.DurationVar(&cfg.idle, "idle-timeout", 30*time.Second, "多久没收到任何帧就踢掉连接;0 表示不踢")
	flag.DurationVar(&cfg.loginTimeout, "login-timeout", 10*time.Second, "连上之后多久之内必须登录")
	flag.DurationVar(&cfg.sessionTTL, "session-ttl", time.Minute, "Redis 会话的 TTL,每 TTL/3 续期一次")
	flag.StringVar(&cfg.node, "node", defaultNode(), "本进程标识,写进会话的 owner")
	logJSON := flag.Bool("log-json", false, "日志输出 JSON(默认是给人看的 text 格式)")
	logLevel := flag.String("log-level", "info", "日志级别:debug / info / warn / error")
	flag.Parse()

	logger := newLogger(*logJSON, *logLevel)
	slog.SetDefault(logger)

	if err := run(logger, cfg); err != nil {
		logger.Error("gameserver exited with error", "err", err)
		os.Exit(1)
	}
	logger.Info("gameserver stopped")
}

func run(logger *slog.Logger, cfg config) error {
	signer, err := auth.NewSigner([]byte(os.Getenv("GS_TOKEN_SECRET")))
	if err != nil {
		return fmt.Errorf("GS_TOKEN_SECRET: %w", err)
	}

	rdb := storage.NewRedis(cfg.redisAddr)
	defer rdb.Close()
	// 启动时 Redis 不通不退出:登录会回 UNAVAILABLE,Redis 起来之后自动恢复。
	// 这样 docker compose 里谁先起来都行,也不会因为 Redis 重启把游戏服一起带走。
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		logger.Warn("redis not reachable yet, logins will fail until it is", "addr", cfg.redisAddr, "err", err)
	}
	cancel()

	// MySQL 可选。和 Redis 一样,启动时连不上只打警告:账号查询会失败(登录回 UNAVAILABLE),
	// 战绩写失败会重试、最后丢弃,MySQL 起来之后自动恢复。
	var accounts lobby.AccountStore
	var records room.RecordSink
	if dsn := os.Getenv("GS_MYSQL_DSN"); dsn != "" {
		db, err := storage.OpenMySQL(dsn)
		if err != nil {
			return fmt.Errorf("GS_MYSQL_DSN: %w", err)
		}
		defer db.Close()
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := db.PingContext(pingCtx); err != nil {
			logger.Warn("mysql not reachable yet, logins will fail until it is", "err", err)
		}
		cancel()
		accounts, records = storage.NewAccounts(db), storage.NewMatchRecords(db)
	} else {
		logger.Warn("GS_MYSQL_DSN not set: accounts are not checked and match records are not saved")
	}

	rooms := room.NewManager(room.Config{
		Capacity:      roomCapacity,
		TickInterval:  tickInterval,
		RecordMatches: records != nil,
		Logger:        logger,
	})
	defer rooms.Close()
	lb := lobby.New(lobby.Config{
		Signer:       signer,
		Sessions:     storage.NewSessions(rdb, cfg.sessionTTL),
		Accounts:     accounts,
		Rooms:        rooms,
		Node:         cfg.node,
		LoginTimeout: cfg.loginTimeout,
		SessionTTL:   cfg.sessionTTL,
		StoreTimeout: time.Second,
		Logger:       logger,
	})
	srv := &gateway.Server{
		Addr:         cfg.addr,
		IdleTimeout:  cfg.idle,
		WriteTimeout: 5 * time.Second,
		Handler:      lb,
		Logger:       logger,
	}

	// Ctrl-C 和 docker stop(SIGTERM)都走这里:取消 ctx → 网关关 listener 和所有连接 →
	// 每条连接的 OnClose 删掉自己的会话 → Serve 返回。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metrics.SetGaugeSources(srv.Count, lb.Online, rooms.Count)
	if cfg.adminAddr != "" {
		stopAdmin, err := serveAdmin(logger, cfg.adminAddr)
		if err != nil {
			return fmt.Errorf("admin listen: %w", err)
		}
		defer stopAdmin()
	}

	// 后台任务用单独的 ctx,等网关关完(所有 OnClose 跑完、房间都空了)才停:
	// 房间镜像停下前最后同步一次,Redis 里就不会留下这一局的房间。
	bgCtx, bgCancel := context.WithCancel(context.Background())
	var bg sync.WaitGroup
	bg.Go(func() { lb.Run(bgCtx) })                                                      // 会话续期
	bg.Go(func() { rooms.RunMirror(bgCtx, storage.NewRoomMirror(rdb), 30*time.Second) }) // 房间镜像
	if records != nil {
		bg.Go(func() { rooms.RunRecorder(bgCtx, records) }) // 战绩:关停时把最后一批解散的房间写完
	}
	err = srv.ListenAndServe(ctx)
	bgCancel()
	bg.Wait()
	return err
}

// serveAdmin 在 addr 上起 /metrics 和 pprof。Listen 失败直接报错退出:端口被占说明配置错了,
// 带着一个看不见指标的服务跑下去更糟。返回的函数关掉它(最多等 2 秒)。
func serveAdmin(logger *slog.Logger, addr string) (func(), error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	hs := &http.Server{Handler: metrics.AdminHandler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := hs.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			logger.Error("admin server stopped", "err", err)
		}
	}()
	logger.Info("admin listening", "addr", ln.Addr().String())
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = hs.Shutdown(ctx)
	}, nil
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
