// botclient 是冒烟 / 压测客户端。
// 现在的版本:单连接,按间隔发 N 个 Ping,逐个等 Pong,统计 RTT 分位数。
// Lab 5 在此基础上加并发连接数和同步流量,产出 500 并发下的 P99。
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"slices"
	"time"

	"github.com/Anthonyz5527083/game-server/internal/gateway"
	"github.com/Anthonyz5527083/game-server/internal/pb"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "服务端地址")
	n := flag.Int("n", 20, "发 Ping 的次数")
	interval := flag.Duration("interval", 100*time.Millisecond, "两次 Ping 之间的间隔")
	timeout := flag.Duration("timeout", 3*time.Second, "连接超时,以及每次等 Pong 的超时")
	quiet := flag.Bool("q", false, "不逐条打印,只打印最后的统计")
	flag.Parse()

	if err := run(*addr, *n, *interval, *timeout, *quiet); err != nil {
		fmt.Fprintln(os.Stderr, "botclient:", err)
		os.Exit(1)
	}
}

func run(addr string, n int, interval, timeout time.Duration, quiet bool) error {
	nc, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer nc.Close()
	r := bufio.NewReader(nc)

	rtts := make([]time.Duration, 0, n)
	for i := 1; i <= n; i++ {
		seq := uint32(i)
		// RTT 用本地单调时钟量(time.Now / time.Since),不依赖服务端时钟;
		// Ping 里的 client_time_ms 只是给服务端回显的字段。
		start := time.Now()
		_ = nc.SetDeadline(start.Add(timeout))

		req := &pb.Envelope{Seq: seq, Payload: &pb.Envelope_Ping{Ping: &pb.Ping{ClientTimeMs: start.UnixMilli()}}}
		if err := gateway.WriteEnvelope(nc, req); err != nil {
			return fmt.Errorf("send ping #%d: %w", seq, err)
		}
		env, err := gateway.ReadEnvelope(r)
		if err != nil {
			return fmt.Errorf("wait pong #%d: %w", seq, err)
		}
		rtt := time.Since(start)
		if env.GetSeq() != seq || env.GetPong() == nil {
			return fmt.Errorf("ping #%d: unexpected reply seq=%d payload=%T", seq, env.GetSeq(), env.GetPayload())
		}
		rtts = append(rtts, rtt)
		if !quiet {
			fmt.Printf("pong seq=%d rtt=%v\n", seq, rtt)
		}
		if i < n {
			time.Sleep(interval)
		}
	}
	printSummary(rtts)
	return nil
}

// printSummary 打印 min / p50 / p99 / max。
func printSummary(rtts []time.Duration) {
	if len(rtts) == 0 {
		return
	}
	slices.Sort(rtts)
	fmt.Printf("pings=%d min=%v p50=%v p99=%v max=%v\n",
		len(rtts), rtts[0], percentile(rtts, 0.50), percentile(rtts, 0.99), rtts[len(rtts)-1])
}

// percentile 用「最近秩」取分位数:sorted[ceil(p*n)-1]。样本少时 p99 就是最大值,不做插值。
func percentile(sorted []time.Duration, p float64) time.Duration {
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	return sorted[max(0, min(idx, len(sorted)-1))]
}
