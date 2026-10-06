// gameserver 是服务端入口:解析 flag、配置结构化日志、启动 TCP 网关,
// 收到 SIGINT / SIGTERM 后关闭全部连接再退出。
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Anthonyz5527083/game-server/internal/gateway"
)

func main() {
	addr := flag.String("addr", ":7777", "TCP 监听地址")
	idle := flag.Duration("idle-timeout", 30*time.Second, "多久没收到任何帧就踢掉连接;0 表示不踢")
	logJSON := flag.Bool("log-json", false, "日志输出 JSON(默认是给人看的 text 格式)")
	logLevel := flag.String("log-level", "info", "日志级别:debug / info / warn / error")
	flag.Parse()

	logger := newLogger(*logJSON, *logLevel)
	slog.SetDefault(logger)

	srv := &gateway.Server{
		Addr:         *addr,
		IdleTimeout:  *idle,
		WriteTimeout: 5 * time.Second,
		Logger:       logger,
	}

	// Ctrl-C 和 docker stop(SIGTERM)都走这里:取消 ctx → 网关关 listener 和所有连接 → 正常退出。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := srv.ListenAndServe(ctx); err != nil {
		logger.Error("gameserver exited with error", "err", err)
		os.Exit(1)
	}
	logger.Info("gameserver stopped")
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
