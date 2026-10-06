// Package gateway 实现 TCP 长连接网关:length-prefixed 帧编解码、
// 每连接的读 / 写循环、心跳(Ping / Pong)应答与 idle 超时踢人。
//
// 分层约定:gateway 只认识 Envelope,不认识业务。除心跳以外的消息
// 通过 Handler 交给上层(登录、房间……)。
package gateway

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"

	"github.com/Anthonyz5527083/game-server/internal/pb"
)

// 帧格式(wire format)。TCP 是字节流、没有消息边界,用长度前缀切分:
//
//	+--------------------+--------------------------------+
//	| length: 4 字节大端 | body: Envelope 的 protobuf 字节 |
//	+--------------------+--------------------------------+
//
// length 只计 body 的字节数,不含自己的 4 字节。
const (
	headerSize = 4

	// MaxFrameSize 是单帧 body 的上限。超过就算协议违规、直接断开;
	// 也防止对端伪造一个巨大的 length,骗服务端一次分配几 GiB。
	MaxFrameSize = 64 << 10 // 64 KiB
)

var (
	ErrFrameTooLarge = errors.New("gateway: frame exceeds MaxFrameSize")
	ErrEmptyFrame    = errors.New("gateway: empty frame")
)

// ReadFrame 从 r 读一帧,返回 body(不含长度头)。
// r 最好是 *bufio.Reader,否则每帧至少两次 read 系统调用(头一次、body 一次)。
//
// 两种 EOF 的含义不同:
//   - io.EOF:对端在帧边界上关闭,一个字节都没多发,是正常断开;
//   - io.ErrUnexpectedEOF:帧读到一半连接断了,说明对端异常或在捣乱。
func ReadFrame(r io.Reader) ([]byte, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err // 0 字节 → io.EOF;读到 1–3 字节 → io.ErrUnexpectedEOF
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return nil, ErrEmptyFrame
	}
	if n > MaxFrameSize {
		// 先校验再分配:这一行挡在 make 前面,恶意长度头才分配不到内存。
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF {
			// 头已经到了、body 一个字节都没来:这是截断,不是正常关闭。
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return body, nil
}

// Marshal 把 env 编码成一整帧(含 4 字节长度头),可以直接写进 net.Conn。
//
// 先 proto.Size 再 MarshalAppend 到预留好容量的 slice 里,整帧只分配一次内存;
// 如果先 proto.Marshal 再 append 到头后面,会多一次分配和一次拷贝。
// 返回值可以被多个连接共享(广播时同一份 []byte 发给房间里所有人)。
func Marshal(env *pb.Envelope) ([]byte, error) {
	size := proto.Size(env)
	if size == 0 {
		return nil, ErrEmptyFrame
	}
	if size > MaxFrameSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, size)
	}
	buf := make([]byte, headerSize, headerSize+size)
	// UseCachedSize:复用上面 proto.Size 算好的长度,不再遍历一遍消息。
	buf, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(buf, env)
	if err != nil {
		return nil, fmt.Errorf("gateway: encode envelope: %w", err)
	}
	binary.BigEndian.PutUint32(buf[:headerSize], uint32(len(buf)-headerSize))
	return buf, nil
}

// Unmarshal 把一帧 body 解码成 Envelope。
func Unmarshal(body []byte) (*pb.Envelope, error) {
	env := &pb.Envelope{}
	if err := proto.Unmarshal(body, env); err != nil {
		return nil, fmt.Errorf("gateway: decode envelope: %w", err)
	}
	return env, nil
}

// WriteEnvelope 编码并写出一帧。给客户端和测试用;
// 服务端这边要走 Conn.Send,由发送队列保证同一条连接上的写是串行的。
func WriteEnvelope(w io.Writer, env *pb.Envelope) error {
	frame, err := Marshal(env)
	if err != nil {
		return err
	}
	_, err = w.Write(frame)
	return err
}

// ReadEnvelope 读一帧并解码。
func ReadEnvelope(r io.Reader) (*pb.Envelope, error) {
	body, err := ReadFrame(r)
	if err != nil {
		return nil, err
	}
	return Unmarshal(body)
}
