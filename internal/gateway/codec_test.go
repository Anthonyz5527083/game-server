package gateway

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"runtime"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/Anthonyz5527083/game-server/internal/pb"
)

func ping(seq uint32, ts int64) *pb.Envelope {
	return &pb.Envelope{Seq: seq, Payload: &pb.Envelope_Ping{Ping: &pb.Ping{ClientTimeMs: ts}}}
}

// T1.1 编码后再解码,字段完全一致。
func TestCodecRoundTrip(t *testing.T) {
	want := ping(42, 1_700_000_000_000)
	frame, err := Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.BigEndian.Uint32(frame); int(got) != len(frame)-headerSize {
		t.Fatalf("length header = %d, body is %d bytes", got, len(frame)-headerSize)
	}
	got, err := ReadEnvelope(bytes.NewReader(frame))
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("round trip mismatch:\n got  %v\n want %v", got, want)
	}
}

// T1.2 + T1.3:各种坏输入分别返回什么错。用 errors.Is 判断,不比字符串。
func TestReadFrameErrors(t *testing.T) {
	frame, err := Marshal(ping(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		input []byte
		want  error
	}{
		{"空输入:对端在帧边界上正常关闭", nil, io.EOF},
		{"半个长度头", frame[:2], io.ErrUnexpectedEOF},
		{"只有长度头", frame[:headerSize], io.ErrUnexpectedEOF},
		{"半个 body", frame[:len(frame)-1], io.ErrUnexpectedEOF},
		{"零长度帧", []byte{0, 0, 0, 0}, ErrEmptyFrame},
		{"恶意长度头 0xFFFFFFFF", []byte{0xFF, 0xFF, 0xFF, 0xFF}, ErrFrameTooLarge},
		{"刚好超上限 1 字节", be32(MaxFrameSize + 1), ErrFrameTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ReadFrame(bytes.NewReader(tt.input))
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

// 恶意长度头不应该触发大块分配:ReadFrame 必须在 make 之前拒绝。
// 看的是分配的总字节数,不是次数(错误包装本身就有几次小分配)。
// 长度头用 64 MiB 而不是 0xFFFFFFFF:检查一旦被改坏,这个测试要能安全地红,而不是分配 100 次 4 GiB 把机器拖死。
func TestReadFrameRejectsBeforeAllocating(t *testing.T) {
	input := be32(64 << 20)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 100 {
		_, _ = ReadFrame(bytes.NewReader(input))
	}
	runtime.ReadMemStats(&after)
	if got := after.TotalAlloc - before.TotalAlloc; got > 1<<20 {
		t.Fatalf("100 calls allocated %d bytes; ReadFrame allocates before checking the length", got)
	}
}

func TestMarshalEmptyEnvelope(t *testing.T) {
	if _, err := Marshal(&pb.Envelope{}); !errors.Is(err, ErrEmptyFrame) {
		t.Fatalf("err = %v, want ErrEmptyFrame", err)
	}
}

// 多帧背靠背放在一个流里(粘包的离线版本),要能逐帧读出来。
func TestReadFramesBackToBack(t *testing.T) {
	var stream []byte
	for seq := uint32(1); seq <= 3; seq++ {
		f, err := Marshal(ping(seq, int64(seq)))
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, f...)
	}
	r := bufio.NewReader(bytes.NewReader(stream))
	for seq := uint32(1); seq <= 3; seq++ {
		env, err := ReadEnvelope(r)
		if err != nil {
			t.Fatal(err)
		}
		if env.GetSeq() != seq {
			t.Fatalf("seq = %d, want %d", env.GetSeq(), seq)
		}
	}
	if _, err := ReadEnvelope(r); !errors.Is(err, io.EOF) {
		t.Fatalf("after last frame: err = %v, want io.EOF", err)
	}
}

func be32(n uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	return b
}

// B1.1 编码一帧 Ping。看 allocs/op:Size + MarshalAppend 应该只分配 1 次。
func BenchmarkMarshalPing(b *testing.B) {
	env := ping(42, 1_700_000_000_000)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Marshal(env); err != nil {
			b.Fatal(err)
		}
	}
}

// B1.2 解码一帧 Ping:bufio.Reader 里放 1024 帧循环读,读完重置。
func BenchmarkReadEnvelopePing(b *testing.B) {
	frame, err := Marshal(ping(42, 1_700_000_000_000))
	if err != nil {
		b.Fatal(err)
	}
	const frames = 1024
	data := bytes.Repeat(frame, frames)
	src := bytes.NewReader(data)
	r := bufio.NewReader(src)
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	i := 0
	for b.Loop() {
		if i == frames {
			src.Reset(data)
			r.Reset(src)
			i = 0
		}
		if _, err := ReadEnvelope(r); err != nil {
			b.Fatal(err)
		}
		i++
	}
}
