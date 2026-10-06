// tokengen 给一个玩家签发登录 token,开发和测试用。密钥从 GS_TOKEN_SECRET 读,和服务端一致。
//
//	GS_TOKEN_SECRET=... tokengen -player 42 -ttl 24h
//
// 真实产品里 token 由账号服务签发(玩家输密码 / 第三方登录之后),游戏服只负责校验。
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Anthonyz5527083/game-server/internal/auth"
)

func main() {
	player := flag.Uint64("player", 0, "玩家 ID(必须 > 0)")
	ttl := flag.Duration("ttl", 24*time.Hour, "token 有效期")
	flag.Parse()

	signer, err := auth.NewSigner([]byte(os.Getenv("GS_TOKEN_SECRET")))
	if err != nil {
		fmt.Fprintln(os.Stderr, "tokengen: GS_TOKEN_SECRET:", err)
		os.Exit(1)
	}
	tok, err := signer.Issue(*player, time.Now().Add(*ttl))
	if err != nil {
		fmt.Fprintln(os.Stderr, "tokengen:", err)
		os.Exit(1)
	}
	fmt.Println(tok)
}
