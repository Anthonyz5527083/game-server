package auth

import (
	"errors"
	"strings"
	"testing"
	"time"
)

var secret = []byte("0123456789abcdef-test-only")

func mustSigner(t *testing.T, key []byte) *Signer {
	t.Helper()
	s, err := NewSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestIssueVerify(t *testing.T) {
	s := mustSigner(t, secret)
	tok, err := s.Issue(42, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := s.Verify(tok)
	if err != nil || pid != 42 {
		t.Fatalf("Verify = %d, %v; want 42, nil", pid, err)
	}
}

// T2.2 改掉任何一个字符都必须被拒,不管改的是内容还是签名。
func TestVerifyRejectsEveryTamperedByte(t *testing.T) {
	s := mustSigner(t, secret)
	tok, err := s.Issue(42, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for i := range len(tok) {
		b := []byte(tok)
		if b[i] == 'A' {
			b[i] = 'B'
		} else {
			b[i] = 'A'
		}
		if pid, err := s.Verify(string(b)); err == nil {
			t.Fatalf("tampered byte %d accepted, pid=%d", i, pid)
		}
	}
}

// T2.3 过期的 token 返回 ErrExpired,和「签名错」能区分开。
func TestVerifyExpired(t *testing.T) {
	s := mustSigner(t, secret)
	tok, err := s.Issue(42, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Verify(tok); !errors.Is(err, ErrExpired) {
		t.Fatalf("err = %v, want ErrExpired", err)
	}
}

func TestVerifyWrongKey(t *testing.T) {
	tok, err := mustSigner(t, secret).Issue(42, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	other := mustSigner(t, []byte("another-secret-that-is-long"))
	if _, err := other.Verify(tok); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("err = %v, want ErrBadSignature", err)
	}
}

func TestVerifyMalformed(t *testing.T) {
	s := mustSigner(t, secret)
	good, err := s.Issue(42, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for _, tok := range []string{
		"",
		".",
		"no-dot-at-all",
		good + ".extra",
		"!!!." + "!!!",
		good + string(make([]byte, maxTokenLen)),
	} {
		if _, err := s.Verify(tok); err == nil {
			t.Errorf("Verify(%q) accepted", tok)
		}
	}
}

func TestSignerRejectsBadInput(t *testing.T) {
	if _, err := NewSigner([]byte("short")); err == nil {
		t.Error("short secret accepted")
	}
	if _, err := mustSigner(t, secret).Issue(0, time.Now().Add(time.Hour)); err == nil {
		t.Error("player id 0 accepted")
	}
}

// 回归测试:签名最后一个字符只有 4 位有效,剩下 2 位是填充。只改填充位的 token 也必须被拒。
func TestVerifyRejectsPaddingBitTamper(t *testing.T) {
	s := mustSigner(t, secret)
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	for pid := uint64(1); pid <= 50; pid++ {
		tok, err := s.Issue(pid, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		last := strings.IndexByte(alphabet, tok[len(tok)-1])
		for flip := 1; flip <= 3; flip++ { // 只动低 2 位
			tampered := tok[:len(tok)-1] + string(alphabet[last^flip])
			if _, err := s.Verify(tampered); err == nil {
				t.Fatalf("pid %d: padding-bit tamper accepted: %q", pid, tampered)
			}
		}
	}
}
