package gateway

import (
	"flag"
	"io"
	"log/slog"
	"os"
	"testing"
)

var showLog = flag.Bool("log", false, "打印网关日志(默认丢弃)")

// 测试里每条连接都会打 accepted / closed 日志,全打出来会淹没失败信息。
// 需要看日志时:go test ./internal/gateway -args -log
func TestMain(m *testing.M) {
	flag.Parse()
	if !*showLog {
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	}
	os.Exit(m.Run())
}
