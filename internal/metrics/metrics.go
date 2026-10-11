// Package metrics 定义服务端的 Prometheus 指标,以及挂 /metrics 和 pprof 的 admin handler。
//
// 指标是包级变量,注册在默认 registry 上:业务代码直接 metrics.TickDuration.Observe(...),
// 不用把 registry 一层层传下去。默认 registry 还自带 go_* 和 process_* 指标
// (goroutine 数、GC、RSS),压测时的资源数据直接从这里读。
//
// 每个指标为什么是这个类型、bucket 为什么这么划,见 docs/DECISIONS.md D21、D22。
package metrics

import (
	"net/http"
	"net/http/pprof"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/Anthonyz5527083/game-server/internal/pb"
)

var (
	// TickDuration:一次 tick 从开始等房间锁,到快照投递给最后一个成员。
	// 正常是几微秒,预算是 50ms;bucket 从 1µs 开始,默认 bucket(5ms 起)会把所有样本塞进第一个桶。
	TickDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gameserver_tick_duration_seconds",
		Help:    "Time to advance one room by one tick and hand the snapshot to every member.",
		Buckets: []float64{1e-6, 2.5e-6, 5e-6, 10e-6, 25e-6, 50e-6, 100e-6, 250e-6, 500e-6, 1e-3, 5e-3, 25e-3},
	})

	// TickLateness:tick goroutine 实际醒来的时刻比理想时刻 start + k·interval 晚了多久。
	// 量的是调度器和 Ticker 的准点程度,满载时会变大;到 50ms 就是整整晚了一个 tick。
	TickLateness = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gameserver_tick_lateness_seconds",
		Help:    "How late a room's tick goroutine woke up relative to its ideal schedule.",
		Buckets: []float64{50e-6, 100e-6, 250e-6, 500e-6, 1e-3, 2e-3, 5e-3, 10e-3, 25e-3, 50e-3},
	})

	messages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gameserver_messages_total",
		Help: "Messages received from / queued to clients, by direction and Envelope payload type.",
	}, []string{"dir", "type"})
)

// payload 是 Envelope 里的 oneof,字段号就是消息类型。
var payload = (&pb.Envelope{}).ProtoReflect().Descriptor().Oneofs().ByName("payload")

// 按字段号预先建好每个 (方向, 类型) 的 counter:每条消息只做一次数组下标,
// 不用每次拿 label 字符串去哈希查找。所有类型一开始就以 0 出现在 /metrics 里,rate() 从第一次抓取就有值。
var (
	in, out               [64]prometheus.Counter
	unknownIn, unknownOut = messages.WithLabelValues("in", "none"), messages.WithLabelValues("out", "none")
)

func init() {
	fields := payload.Fields()
	for i := range fields.Len() {
		f := fields.Get(i)
		in[f.Number()] = messages.WithLabelValues("in", string(f.Name()))
		out[f.Number()] = messages.WithLabelValues("out", string(f.Name()))
	}
}

// Received 记一条收到的消息。
func Received(env *pb.Envelope) { counter(&in, unknownIn, env).Inc() }

// Sent 记 n 条投递进发送队列的同类消息。广播时一帧发给 n 个人,记 n 条。
func Sent(env *pb.Envelope, n int) {
	if n > 0 {
		counter(&out, unknownOut, env).Add(float64(n))
	}
}

func counter(tab *[64]prometheus.Counter, unknown prometheus.Counter, env *pb.Envelope) prometheus.Counter {
	fd := env.ProtoReflect().WhichOneof(payload)
	if fd == nil { // 没有 payload 的 Envelope(大厅会按协议错误断开它)
		return unknown
	}
	if n := fd.Number(); n < protoreflect.FieldNumber(len(tab)) && tab[n] != nil {
		return tab[n]
	}
	return unknown
}

// RegisterGauges 注册三个 gauge。值在被抓取时现算,不用在每次上下线时维护计数,也就不会和真实状态对不上。
// 一个进程只能调一次(重复注册会 panic)。
func RegisterGauges(connections, players, rooms func() int) {
	gauge := func(name, help string, f func() int) {
		promauto.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 { return float64(f()) })
	}
	gauge("gameserver_connections", "Open TCP connections, logged in or not.", connections)
	gauge("gameserver_players_online", "Logged-in players.", players)
	gauge("gameserver_rooms", "Rooms that currently exist.", rooms)
}

// AdminHandler 返回挂着 /metrics 和 /debug/pprof/ 的 handler。
// pprof 能看到所有 goroutine 的堆栈、能触发 30 秒的 CPU profile,所以这个端口只能给本机 / 内网。
// 显式注册,不用 import _ "net/http/pprof":那样会挂到 http.DefaultServeMux 上,谁用了默认 mux 谁就把 pprof 暴露了。
func AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index) // heap、goroutine、allocs 等具名 profile 也由它分发
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
