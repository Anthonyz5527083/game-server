// Package auth 签发和校验登录 token。
//
// token 是无状态的:服务端只要有密钥就能校验,不用查任何存储(D6)。格式:
//
//	base64url(playerID 8 字节大端 | 过期时间 unix 秒 8 字节大端) "." base64url(HMAC-SHA256(密钥, 点号前那一段))
//
// 只用标准库。没用 JWT:JWT 的 header 里能指定算法,历史上出过「alg: none」一类的漏洞,
// 这里只有一种算法,自己拼 30 行比引一个库更容易讲清楚。
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"
	"time"
)

var (
	ErrMalformed    = errors.New("auth: malformed token")
	ErrBadSignature = errors.New("auth: bad token signature")
	ErrExpired      = errors.New("auth: token expired")
)

// MinSecretLen 是密钥的最短长度。HMAC-SHA256 的密钥再短就容易被暴力猜出来。
const MinSecretLen = 16

const (
	payloadLen  = 16  // playerID 8 + expiresAt 8
	maxTokenLen = 128 // 正常 token 是 22 + 1 + 43 = 66 字符;超长的直接拒,不去解码
)

// Strict:拒绝非零的填充位。43 个字符能装 258 位,而签名只有 256 位,最后一个字符有 2 位是填充;
// 默认的解码器忽略这 2 位,于是同一个签名能写成好几种字符串,改了最后一个字符的 token 照样通过。
// 这个坑是测试连跑时撞出来的(见 DECISIONS D6)。
var b64 = base64.RawURLEncoding.Strict()

// Signer 用同一个密钥签发和校验 token。可以并发使用。
type Signer struct {
	key []byte
	now func() time.Time
}

func NewSigner(secret []byte) (*Signer, error) {
	if len(secret) < MinSecretLen {
		return nil, errors.New("auth: secret must be at least 16 bytes")
	}
	return &Signer{key: append([]byte(nil), secret...), now: time.Now}, nil
}

// Issue 给 playerID 签发一个在 expiresAt 过期的 token。playerID 0 保留为「未登录」,不能签。
func (s *Signer) Issue(playerID uint64, expiresAt time.Time) (string, error) {
	if playerID == 0 {
		return "", errors.New("auth: player id 0 is reserved")
	}
	var payload [payloadLen]byte
	binary.BigEndian.PutUint64(payload[:8], playerID)
	binary.BigEndian.PutUint64(payload[8:], uint64(expiresAt.Unix()))
	body := b64.EncodeToString(payload[:])
	return body + "." + b64.EncodeToString(s.mac(body)), nil
}

// Verify 校验 token,返回其中的 playerID。
//
// 顺序有讲究:先验签名,再解析内容。签名没过的数据一个字节都不信,
// 包括里面的过期时间:否则攻击者可以通过改过期时间探测出不同的错误。
func (s *Signer) Verify(token string) (uint64, error) {
	if len(token) > maxTokenLen {
		return 0, ErrMalformed
	}
	body, sigB64, ok := strings.Cut(token, ".")
	if !ok || strings.Contains(sigB64, ".") {
		return 0, ErrMalformed
	}
	sig, err := b64.DecodeString(sigB64)
	if err != nil {
		return 0, ErrMalformed
	}
	// hmac.Equal 是常数时间比较:用 bytes.Equal 的话,比较在第一个不同的字节就返回,
	// 攻击者能从响应时间一个字节一个字节地猜出正确签名。
	if !hmac.Equal(sig, s.mac(body)) {
		return 0, ErrBadSignature
	}
	payload, err := b64.DecodeString(body)
	if err != nil || len(payload) != payloadLen {
		return 0, ErrMalformed
	}
	playerID := binary.BigEndian.Uint64(payload[:8])
	expiresAt := int64(binary.BigEndian.Uint64(payload[8:]))
	if playerID == 0 {
		return 0, ErrMalformed
	}
	if s.now().Unix() >= expiresAt {
		return 0, ErrExpired
	}
	return playerID, nil
}

func (s *Signer) mac(body string) []byte {
	h := hmac.New(sha256.New, s.key)
	h.Write([]byte(body))
	return h.Sum(nil)
}
