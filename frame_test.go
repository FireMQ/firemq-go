package firemq

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"
	"testing/iotest"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type vectorFile struct {
	Vectors []struct {
		Name       string         `json:"name"`
		Op         byte           `json:"op"`
		OpName     string         `json:"op_name"`
		Header     map[string]any `json:"header"`
		PayloadHex string         `json:"payload_hex"`
		FrameHex   string         `json:"frame_hex"`
	} `json:"vectors"`
}

// normalize turns a value into what encoding/json produces for it, so MessagePack and
// JSON values compare equal.
func normalize(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// dropZero removes map entries holding a zero value, since the protocol treats an
// omitted field and its zero value the same.
func dropZero(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}
	out := map[string]any{}
	for k, val := range m {
		val = dropZero(val)
		switch x := val.(type) {
		case nil:
			continue
		case bool:
			if !x {
				continue
			}
		case float64:
			if x == 0 {
				continue
			}
		case string:
			if x == "" {
				continue
			}
		case map[string]any:
			if len(x) == 0 {
				continue
			}
		case []any:
			if len(x) == 0 {
				continue
			}
		}
		out[k] = val
	}
	return out
}

func genericHeader(t *testing.T, h []byte) map[string]any {
	t.Helper()
	m := map[string]any{}
	if err := decodeHeader(h, &m); err != nil {
		t.Fatalf("decoding header as a map: %v", err)
	}
	return m
}

func TestVectorsDecode(t *testing.T) {
	data, err := os.ReadFile("testdata/frames.json")
	if err != nil {
		t.Fatal(err)
	}
	var file vectorFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Vectors) == 0 {
		t.Fatal("no vectors")
	}
	for _, v := range file.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			raw, err := hex.DecodeString(v.FrameHex)
			if err != nil {
				t.Fatal(err)
			}
			f, err := readFrame(bytes.NewReader(raw), math.MaxUint32)
			if err != nil {
				t.Fatalf("readFrame: %v", err)
			}
			if f.op != v.Op || opName(f.op) != v.OpName {
				t.Fatalf("op = %#x (%s), want %#x (%s)", f.op, opName(f.op), v.Op, v.OpName)
			}
			if got := hex.EncodeToString(f.payload); got != v.PayloadHex {
				t.Fatalf("payload = %s, want %s", got, v.PayloadHex)
			}
			want := normalize(t, v.Header)
			if got := normalize(t, genericHeader(t, f.header)); !reflect.DeepEqual(got, want) {
				t.Fatalf("header = %v, want %v", got, want)
			}

			// The typed decoder keeps every field the vector sets.
			typed, err := decodeCommand(f)
			if err != nil {
				t.Fatalf("decodeCommand: %v", err)
			}
			var reencoded []byte
			if typed != nil {
				reencoded, err = encodeFrame(f.op, typed, f.payload)
			} else {
				reencoded, err = encodeFrame(f.op, nil, f.payload)
			}
			if err != nil {
				t.Fatal(err)
			}
			g, err := readFrame(bytes.NewReader(reencoded), math.MaxUint32)
			if err != nil {
				t.Fatal(err)
			}
			got := dropZero(normalize(t, genericHeader(t, g.header)))
			if !reflect.DeepEqual(got, dropZero(want)) {
				t.Fatalf("typed round trip = %v, want %v", got, dropZero(want))
			}
			if !bytes.Equal(g.payload, f.payload) {
				t.Fatalf("payload changed in round trip")
			}

			// WebSocket framing is the same frame without the length prefix.
			ws, err := parseFrame(raw[4:])
			if err != nil || ws.op != f.op || !bytes.Equal(ws.header, f.header) || !bytes.Equal(ws.payload, f.payload) {
				t.Fatalf("parseFrame(ws) = %+v, %v", ws, err)
			}
		})
	}
}

func roundTrip(t *testing.T, op byte, header any, payload []byte) (any, []byte) {
	t.Helper()
	b, err := encodeFrame(op, header, payload)
	if err != nil {
		t.Fatal(err)
	}
	f, err := readFrame(bytes.NewReader(b), math.MaxUint32)
	if err != nil {
		t.Fatal(err)
	}
	h, err := decodeCommand(f)
	if err != nil {
		t.Fatal(err)
	}
	return h, f.payload
}

func TestCommandsRoundTrip(t *testing.T) {
	// Values as the decoder returns them, before normalizeTable.
	headers := map[string]any{
		"x-retries": int8(3),
		"neg":       int8(-7),
		"big":       uint64(math.MaxUint64),
		"ratio":     0.25,
		"tags":      []any{"a", "b"},
		"nested":    map[string]any{"ok": true, "none": nil},
		"raw":       []byte{0xff, 0x00},
	}
	cases := []struct {
		op      byte
		header  any
		payload []byte
	}{
		{opHello, &helloHeader{Ver: 1, Vhost: "/", Auth: authHeader{Mech: "plain", User: "guest", Pass: "guest"}, Heartbeat: 30, Client: map[string]any{"name": "firemq-go"}, Caps: []string{"confirms"}}, nil},
		{opPublish, &publishHeader{Seq: 42, Exchange: "orders", Key: "order.created", Mandatory: true, Persistent: true, Props: wireProps{ContentType: "application/json", Expiration: 60000, Priority: 9}, Headers: headers}, []byte(`{"id":1}`)},
		{opDeliver, &deliverHeader{Sub: 1, Tag: 7, Redelivered: true, Key: "q", Headers: headers}, []byte("body")},
		{opConfirm, &confirmHeader{Upto: 10}, nil},
		{opConfirm, &confirmHeader{Seq: 4, Nack: true, Code: 406, Msg: "full"}, nil},
		{opOk, &okHeader{Rid: 3, Name: "amq.gen-x", Messages: 5, Consumers: 1}, nil},
		{opError, &errorHeader{Code: 406, Msg: "unknown delivery tag 9"}, nil},
		{opGetOk, &getOkHeader{Rid: 8, Tag: 9, Key: "jobs", Remaining: 3}, []byte{0xff, 0xfe}},
		{opBind, &bindHeader{Rid: 4, Exchange: "amq.headers", Queue: "q", Args: map[string]any{"x-match": "all"}}, nil},
	}
	for _, tc := range cases {
		got, payload := roundTrip(t, tc.op, tc.header, tc.payload)
		if !reflect.DeepEqual(got, tc.header) {
			t.Errorf("%s: got %#v, want %#v", opName(tc.op), got, tc.header)
		}
		if !bytes.Equal(payload, tc.payload) {
			t.Errorf("%s: payload %q, want %q", opName(tc.op), payload, tc.payload)
		}
	}
	h, _ := roundTrip(t, opPing, nil, nil)
	if h != nil {
		t.Errorf("ping decoded to %#v", h)
	}
}

func TestHeaderIntegersDecodeAsInt64(t *testing.T) {
	in := map[string]any{
		"small": 2, "int8": int8(-3), "u16": uint16(300), "u64": uint64(math.MaxUint64),
		"f32": float32(0.5), "list": []any{uint8(200), "s"}, "map": map[string]any{"n": int32(-70000)},
		"bin": []byte{1, 2}, "str": "x",
	}
	h, _ := roundTrip(t, opDeliver, &deliverHeader{Headers: in}, nil)
	got := normalizeTable(h.(*deliverHeader).Headers)
	want := map[string]any{
		"small": int64(2), "int8": int64(-3), "u16": int64(300), "u64": uint64(math.MaxUint64),
		"f32": float64(0.5), "list": []any{int64(200), "s"}, "map": map[string]any{"n": int64(-70000)},
		"bin": []byte{1, 2}, "str": "x",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("headers = %#v, want %#v", got, want)
	}
}

func TestFramesSplitAcrossReads(t *testing.T) {
	var stream []byte
	a, _ := encodeFrame(opPublish, &publishHeader{Key: "k"}, []byte("hello"))
	b, _ := encodeFrame(opPing, nil, nil)
	stream = append(append(stream, a...), b...)
	r := iotest.OneByteReader(bytes.NewReader(stream))
	f1, err := readFrame(r, 1024)
	if err != nil || f1.op != opPublish || string(f1.payload) != "hello" {
		t.Fatalf("first frame = %+v, %v", f1, err)
	}
	f2, err := readFrame(r, 1024)
	if err != nil || f2.op != opPing || len(f2.header) != 0 {
		t.Fatalf("second frame = %+v, %v", f2, err)
	}
}

func TestOversizedFramesAreRejected(t *testing.T) {
	b, _ := encodeFrame(opPublish, nil, make([]byte, 100))
	if _, err := readFrame(bytes.NewReader(b), 50); err == nil {
		t.Fatal("expected an error")
	}
}

func TestMalformedFrames(t *testing.T) {
	if _, err := parseFrame([]byte{0x20, 0, 0, 0, 9, 1}); err == nil {
		t.Fatal("header longer than frame: expected an error")
	}
	if _, err := readFrame(bytes.NewReader([]byte{0, 0, 0, 2, 3, 0}), 1024); err == nil {
		t.Fatal("frame shorter than 5 bytes: expected an error")
	}
}

func TestOmittedFieldsTakeZeroValues(t *testing.T) {
	header, _ := msgpack.Marshal(map[string]any{"queue": "jobs"})
	h, err := decodeCommand(frame{op: opSubscribe, header: header})
	if err != nil {
		t.Fatal(err)
	}
	if got := h.(*subscribeHeader); !reflect.DeepEqual(*got, subscribeHeader{Queue: "jobs"}) {
		t.Fatalf("got %+v", got)
	}
}

func TestUnknownKeysAreIgnored(t *testing.T) {
	header, _ := msgpack.Marshal(map[string]any{"tag": 5, "future_field": []int{1, 2}})
	h, err := decodeCommand(frame{op: opAck, header: header})
	if err != nil {
		t.Fatal(err)
	}
	if got := h.(*ackHeader); *got != (ackHeader{Tag: 5}) {
		t.Fatalf("got %+v", got)
	}
}

func TestUnknownOpsAreReported(t *testing.T) {
	if _, err := decodeCommand(frame{op: 0x7f}); err == nil {
		t.Fatal("expected an error")
	}
}

func TestPropertiesConversion(t *testing.T) {
	p := Properties{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		CorrelationID:   "c-1",
		ReplyTo:         "replies",
		Expiration:      1500 * time.Millisecond,
		MessageID:       "m-1",
		Timestamp:       time.Unix(1767225600, 0).UTC(),
		Type:            "greeting",
		UserID:          "guest",
		AppID:           "tests",
		Priority:        5,
	}
	w := p.toWire()
	if w.Expiration != 1500 || w.Timestamp != 1767225600 {
		t.Fatalf("wire = %+v", w)
	}
	if got := w.toProperties(); got != p {
		t.Fatalf("got %+v, want %+v", got, p)
	}
	if (Properties{}).toWire() != (wireProps{}) {
		t.Fatal("zero properties must encode as an empty map")
	}
}
