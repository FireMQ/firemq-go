package firemq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

// magic is what a TCP client sends before its first frame.
var magic = []byte("FMQ1")

const (
	// maxHelloFrame bounds frames exchanged before welcome.
	maxHelloFrame = 65536
	// defaultMaxFrame is the frame limit when the server does not announce one.
	defaultMaxFrame = 16 * 1024 * 1024
	// frameOverhead is the op byte and the header length that follow the length prefix.
	frameOverhead = 5
)

// Op codes.
const (
	opHello           byte = 0x01
	opWelcome         byte = 0x02
	opPing            byte = 0x03
	opPong            byte = 0x04
	opClose           byte = 0x05
	opOk              byte = 0x06
	opError           byte = 0x07
	opFlow            byte = 0x08
	opDeclareExchange byte = 0x10
	opDeleteExchange  byte = 0x11
	opDeclareQueue    byte = 0x12
	opDeleteQueue     byte = 0x13
	opPurgeQueue      byte = 0x14
	opBind            byte = 0x15
	opUnbind          byte = 0x16
	opPublish         byte = 0x20
	opConfirm         byte = 0x21
	opReturn          byte = 0x22
	opSubscribe       byte = 0x30
	opUnsubscribe     byte = 0x31
	opCredit          byte = 0x32
	opDeliver         byte = 0x33
	opCancelled       byte = 0x34
	opAck             byte = 0x35
	opNack            byte = 0x36
	opGet             byte = 0x37
	opGetOk           byte = 0x38
)

var opNames = map[byte]string{
	opHello:           "hello",
	opWelcome:         "welcome",
	opPing:            "ping",
	opPong:            "pong",
	opClose:           "close",
	opOk:              "ok",
	opError:           "error",
	opFlow:            "flow",
	opDeclareExchange: "declare_exchange",
	opDeleteExchange:  "delete_exchange",
	opDeclareQueue:    "declare_queue",
	opDeleteQueue:     "delete_queue",
	opPurgeQueue:      "purge_queue",
	opBind:            "bind",
	opUnbind:          "unbind",
	opPublish:         "publish",
	opConfirm:         "confirm",
	opReturn:          "return",
	opSubscribe:       "subscribe",
	opUnsubscribe:     "unsubscribe",
	opCredit:          "credit",
	opDeliver:         "deliver",
	opCancelled:       "cancelled",
	opAck:             "ack",
	opNack:            "nack",
	opGet:             "get",
	opGetOk:           "get_ok",
}

func opName(op byte) string {
	if name, ok := opNames[op]; ok {
		return name
	}
	return fmt.Sprintf("unknown(%#02x)", op)
}

// frame is one protocol frame with its header still MessagePack-encoded.
type frame struct {
	op      byte
	header  []byte
	payload []byte
}

var errMalformed = errors.New("firemq: malformed frame")

// parseFrame splits a frame without its length prefix (a WebSocket message, or what
// follows the prefix on TCP) into op, header and payload. The result aliases b.
func parseFrame(b []byte) (frame, error) {
	if len(b) < frameOverhead {
		return frame{}, fmt.Errorf("%w: shorter than %d bytes", errMalformed, frameOverhead)
	}
	hlen := binary.BigEndian.Uint32(b[1:5])
	if uint64(hlen) > uint64(len(b)-frameOverhead) {
		return frame{}, fmt.Errorf("%w: header length exceeds frame", errMalformed)
	}
	end := frameOverhead + int(hlen)
	return frame{op: b[0], header: b[frameOverhead:end], payload: b[end:]}, nil
}

// readFrame reads one length-prefixed frame. maxFrame bounds the value of the length
// prefix.
func readFrame(r io.Reader, maxFrame uint32) (frame, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return frame{}, err
	}
	n := binary.BigEndian.Uint32(prefix[:])
	if n > maxFrame {
		return frame{}, fmt.Errorf("firemq: frame of %d bytes exceeds the maximum of %d bytes", n, maxFrame)
	}
	if n < frameOverhead {
		return frame{}, fmt.Errorf("%w: shorter than %d bytes", errMalformed, frameOverhead)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return frame{}, err
	}
	return parseFrame(buf)
}

// encodeFrame encodes a length-prefixed frame. A nil header encodes as an empty
// header (hlen 0).
func encodeFrame(op byte, header any, payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(64 + len(payload))
	buf.Write([]byte{0, 0, 0, 0, op, 0, 0, 0, 0})
	if header != nil {
		enc := msgpack.GetEncoder()
		enc.Reset(&buf)
		enc.UseCompactInts(true)
		err := enc.Encode(header)
		msgpack.PutEncoder(enc)
		if err != nil {
			return nil, fmt.Errorf("firemq: encoding %s header: %w", opName(op), err)
		}
	}
	hlen := buf.Len() - 9
	buf.Write(payload)
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b[0:4], uint32(len(b)-4))
	binary.BigEndian.PutUint32(b[5:9], uint32(hlen))
	return b, nil
}

var readerPool = sync.Pool{New: func() any { return new(bytes.Reader) }}

// decodeHeader decodes a MessagePack header into v. An empty header leaves v at its
// zero value. Unknown keys are ignored.
func decodeHeader(h []byte, v any) error {
	if len(h) == 0 {
		return nil
	}
	r := readerPool.Get().(*bytes.Reader)
	r.Reset(h)
	dec := msgpack.GetDecoder()
	dec.Reset(r)
	err := dec.Decode(v)
	msgpack.PutDecoder(dec)
	r.Reset(nil)
	readerPool.Put(r)
	return err
}

// normalizeTable converts the values of a decoded MessagePack map in place so callers
// see one type per kind: integers become int64 (uint64 above math.MaxInt64) and
// float32 becomes float64. Strings stay strings and binary stays []byte.
func normalizeTable(m map[string]any) map[string]any {
	for k, v := range m {
		m[k] = normalizeValue(v)
	}
	return m
}

func normalizeValue(v any) any {
	switch x := v.(type) {
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint:
		return normalizeValue(uint64(x))
	case uint64:
		if x <= math.MaxInt64 {
			return int64(x)
		}
		return x
	case float32:
		return float64(x)
	case []any:
		for i := range x {
			x[i] = normalizeValue(x[i])
		}
		return x
	case map[string]any:
		return normalizeTable(x)
	}
	return v
}
