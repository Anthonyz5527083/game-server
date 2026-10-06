// botclient 是冒烟 / 压测客户端。
// 现在的版本:单连接,(可选)先登录、快速匹配进房间,然后按间隔发 N 个 Ping,逐个等 Pong,统计 RTT 分位数。
// 等 Pong 的时候收到的推送(别人进出房间、被踢)会打印出来,开两个 bot 就能看到彼此。
// Lab 5 在此基础上加并发连接数和同步流量,产出 500 并发下的 P99。
//
// 登录用的 token:给了 -token 就用它;否则用 GS_TOKEN_SECRET 给 -player 现签一个。
// bot 是测试工具,手里有密钥可以自签,省得压测前先跑 500 次 tokengen。
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

	"github.com/Anthonyz5527083/game-server/internal/auth"
	"github.com/Anthonyz5527083/game-server/internal/gateway"
	"github.com/Anthonyz5527083/game-server/internal/pb"
)

type options struct {
	addr     string
	player   uint64
	token    string
	n        int
	interval time.Duration
	timeout  time.Duration
	quiet    bool
	match    bool
}

func main() {
	var o options
	flag.StringVar(&o.addr, "addr", "127.0.0.1:7777", "服务端地址")
	flag.Uint64Var(&o.player, "player", 0, "用这个玩家 ID 登录(0 表示不登录,只发 Ping)")
	flag.StringVar(&o.token, "token", "", "登录用的 token;为空时用 GS_TOKEN_SECRET 给 -player 现签")
	flag.IntVar(&o.n, "n", 20, "发 Ping 的次数")
	flag.DurationVar(&o.interval, "interval", 100*time.Millisecond, "两次 Ping 之间的间隔")
	flag.DurationVar(&o.timeout, "timeout", 3*time.Second, "连接超时,以及每次等应答的超时")
	flag.BoolVar(&o.quiet, "q", false, "不逐条打印 Pong,只打印最后的统计")
	flag.BoolVar(&o.match, "match", false, "登录后快速匹配进一个房间(需要 -player 或 -token)")
	flag.Parse()

	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "botclient:", err)
		os.Exit(1)
	}
}

// client 包一条连接:发请求时自动编 seq,等应答时把中途收到的推送消息打印出来。
type client struct {
	nc      net.Conn
	r       *bufio.Reader
	seq     uint32
	timeout time.Duration
}

func (c *client) send(env *pb.Envelope) (uint32, error) {
	c.seq++
	env.Seq = c.seq
	_ = c.nc.SetDeadline(time.Now().Add(c.timeout))
	return c.seq, gateway.WriteEnvelope(c.nc, env)
}

// await 读到 seq 对应的应答为止。服务端主动推送的消息(seq 0,比如 Kick)打印出来。
func (c *client) await(seq uint32) (*pb.Envelope, error) {
	for {
		env, err := gateway.ReadEnvelope(c.r)
		if err != nil {
			return nil, err
		}
		if env.GetSeq() == seq {
			return env, nil
		}
		fmt.Printf("push: %v\n", env)
	}
}

func run(o options) error {
	nc, err := net.DialTimeout("tcp", o.addr, o.timeout)
	if err != nil {
		return fmt.Errorf("dial %s: %w", o.addr, err)
	}
	defer nc.Close()
	c := &client{nc: nc, r: bufio.NewReader(nc), timeout: o.timeout}

	if o.player != 0 || o.token != "" {
		if err := login(c, o); err != nil {
			return err
		}
		if o.match {
			if err := quickMatch(c); err != nil {
				return err
			}
		}
	}
	return pingLoop(c, o)
}

func login(c *client, o options) error {
	tok := o.token
	if tok == "" {
		signer, err := auth.NewSigner([]byte(os.Getenv("GS_TOKEN_SECRET")))
		if err != nil {
			return fmt.Errorf("sign token: GS_TOKEN_SECRET: %w", err)
		}
		if tok, err = signer.Issue(o.player, time.Now().Add(time.Hour)); err != nil {
			return err
		}
	}
	seq, err := c.send(&pb.Envelope{Payload: &pb.Envelope_LoginReq{LoginReq: &pb.LoginReq{Token: tok}}})
	if err != nil {
		return fmt.Errorf("send login: %w", err)
	}
	env, err := c.await(seq)
	if err != nil {
		return fmt.Errorf("wait login: %w", err)
	}
	resp := env.GetLoginResp()
	if resp.GetCode() != pb.Code_OK {
		return fmt.Errorf("login failed: %v", resp.GetCode())
	}
	fmt.Printf("logged in as player %d\n", resp.GetPlayerId())
	return nil
}

func quickMatch(c *client) error {
	seq, err := c.send(&pb.Envelope{Payload: &pb.Envelope_QuickMatchReq{QuickMatchReq: &pb.QuickMatchReq{}}})
	if err != nil {
		return fmt.Errorf("send quick match: %w", err)
	}
	env, err := c.await(seq)
	if err != nil {
		return fmt.Errorf("wait quick match: %w", err)
	}
	resp := env.GetRoomResp()
	if resp.GetCode() != pb.Code_OK {
		return fmt.Errorf("quick match failed: %v", resp.GetCode())
	}
	fmt.Printf("in room %d, members %v\n", resp.GetRoom().GetRoomId(), resp.GetRoom().GetMembers())
	return nil
}

func pingLoop(c *client, o options) error {
	rtts := make([]time.Duration, 0, o.n)
	for i := 1; i <= o.n; i++ {
		// RTT 用本地单调时钟量(time.Now / time.Since),不依赖服务端时钟;
		// Ping 里的 client_time_ms 只是给服务端回显的字段。
		start := time.Now()
		seq, err := c.send(&pb.Envelope{Payload: &pb.Envelope_Ping{Ping: &pb.Ping{ClientTimeMs: start.UnixMilli()}}})
		if err != nil {
			return fmt.Errorf("send ping #%d: %w", i, err)
		}
		env, err := c.await(seq)
		if err != nil {
			return fmt.Errorf("wait pong #%d: %w", i, err)
		}
		rtt := time.Since(start)
		if env.GetPong() == nil {
			return fmt.Errorf("ping #%d: unexpected reply %T", i, env.GetPayload())
		}
		rtts = append(rtts, rtt)
		if !o.quiet {
			fmt.Printf("pong seq=%d rtt=%v\n", seq, rtt)
		}
		if i < o.n {
			time.Sleep(o.interval)
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
