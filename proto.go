package firemq

import "fmt"

// Header types for each op. Field names follow the protocol spec. Fields marked
// omitempty are left out at their zero value, which decoders read back as the same
// zero value.

const protocolVersion = 1

type authHeader struct {
	Mech string `msgpack:"mech"`
	User string `msgpack:"user,omitempty"`
	Pass string `msgpack:"pass,omitempty"`
}

type helloHeader struct {
	Ver       uint32         `msgpack:"ver"`
	Vhost     string         `msgpack:"vhost,omitempty"`
	Auth      authHeader     `msgpack:"auth"`
	Heartbeat uint32         `msgpack:"heartbeat"`
	Client    map[string]any `msgpack:"client,omitempty"`
	Caps      []string       `msgpack:"caps,omitempty"`
}

type serverInfoHeader struct {
	Name    string `msgpack:"name"`
	Version string `msgpack:"version"`
}

type welcomeHeader struct {
	Ver       uint32           `msgpack:"ver"`
	Server    serverInfoHeader `msgpack:"server"`
	ConnID    string           `msgpack:"conn_id"`
	Heartbeat uint32           `msgpack:"heartbeat"`
	MaxFrame  uint32           `msgpack:"max_frame"`
	Caps      []string         `msgpack:"caps,omitempty"`
}

type closeHeader struct {
	Code uint16 `msgpack:"code"`
	Msg  string `msgpack:"msg,omitempty"`
}

type okHeader struct {
	Rid       uint32 `msgpack:"rid"`
	Name      string `msgpack:"name,omitempty"`
	Messages  uint64 `msgpack:"messages,omitempty"`
	Consumers uint32 `msgpack:"consumers,omitempty"`
	Empty     bool   `msgpack:"empty,omitempty"`
}

type errorHeader struct {
	Rid   uint32 `msgpack:"rid,omitempty"`
	Code  uint16 `msgpack:"code"`
	Msg   string `msgpack:"msg"`
	Fatal bool   `msgpack:"fatal,omitempty"`
}

type flowHeader struct {
	Pause bool `msgpack:"pause"`
}

type declareExchangeHeader struct {
	Rid        uint32         `msgpack:"rid"`
	Name       string         `msgpack:"name"`
	Type       string         `msgpack:"type"`
	Durable    bool           `msgpack:"durable,omitempty"`
	AutoDelete bool           `msgpack:"auto_delete,omitempty"`
	Internal   bool           `msgpack:"internal,omitempty"`
	Passive    bool           `msgpack:"passive,omitempty"`
	Args       map[string]any `msgpack:"args,omitempty"`
}

type deleteExchangeHeader struct {
	Rid      uint32 `msgpack:"rid"`
	Name     string `msgpack:"name"`
	IfUnused bool   `msgpack:"if_unused,omitempty"`
}

type declareQueueHeader struct {
	Rid        uint32         `msgpack:"rid"`
	Name       string         `msgpack:"name,omitempty"`
	Durable    bool           `msgpack:"durable,omitempty"`
	Exclusive  bool           `msgpack:"exclusive,omitempty"`
	AutoDelete bool           `msgpack:"auto_delete,omitempty"`
	Passive    bool           `msgpack:"passive,omitempty"`
	Args       map[string]any `msgpack:"args,omitempty"`
}

type deleteQueueHeader struct {
	Rid      uint32 `msgpack:"rid"`
	Name     string `msgpack:"name"`
	IfUnused bool   `msgpack:"if_unused,omitempty"`
	IfEmpty  bool   `msgpack:"if_empty,omitempty"`
}

type purgeQueueHeader struct {
	Rid  uint32 `msgpack:"rid"`
	Name string `msgpack:"name"`
}

// bindHeader is the header of both bind and unbind.
type bindHeader struct {
	Rid        uint32         `msgpack:"rid"`
	Exchange   string         `msgpack:"exchange"`
	Queue      string         `msgpack:"queue,omitempty"`
	ToExchange string         `msgpack:"to_exchange,omitempty"`
	Key        string         `msgpack:"key,omitempty"`
	Args       map[string]any `msgpack:"args,omitempty"`
}

// wireProps is the props map of publish, return, deliver and get_ok.
type wireProps struct {
	ContentType     string `msgpack:"content_type,omitempty"`
	ContentEncoding string `msgpack:"content_encoding,omitempty"`
	CorrelationID   string `msgpack:"correlation_id,omitempty"`
	ReplyTo         string `msgpack:"reply_to,omitempty"`
	Expiration      uint64 `msgpack:"expiration,omitempty"`
	MessageID       string `msgpack:"message_id,omitempty"`
	Timestamp       uint64 `msgpack:"timestamp,omitempty"`
	Type            string `msgpack:"type,omitempty"`
	UserID          string `msgpack:"user_id,omitempty"`
	AppID           string `msgpack:"app_id,omitempty"`
	Priority        uint8  `msgpack:"priority,omitempty"`
}

type publishHeader struct {
	Seq        uint64         `msgpack:"seq,omitempty"`
	Exchange   string         `msgpack:"exchange,omitempty"`
	Key        string         `msgpack:"key,omitempty"`
	Mandatory  bool           `msgpack:"mandatory,omitempty"`
	Persistent bool           `msgpack:"persistent,omitempty"`
	Props      wireProps      `msgpack:"props,omitempty"`
	Headers    map[string]any `msgpack:"headers,omitempty"`
}

type confirmHeader struct {
	Upto uint64 `msgpack:"upto,omitempty"`
	Seq  uint64 `msgpack:"seq,omitempty"`
	Nack bool   `msgpack:"nack,omitempty"`
	Code uint16 `msgpack:"code,omitempty"`
	Msg  string `msgpack:"msg,omitempty"`
}

type returnHeader struct {
	Seq      uint64         `msgpack:"seq"`
	Code     uint16         `msgpack:"code"`
	Msg      string         `msgpack:"msg"`
	Exchange string         `msgpack:"exchange,omitempty"`
	Key      string         `msgpack:"key,omitempty"`
	Props    wireProps      `msgpack:"props,omitempty"`
	Headers  map[string]any `msgpack:"headers,omitempty"`
}

type subscribeHeader struct {
	Rid       uint32         `msgpack:"rid"`
	Sub       uint32         `msgpack:"sub"`
	Queue     string         `msgpack:"queue"`
	Credit    uint32         `msgpack:"credit"`
	NoAck     bool           `msgpack:"no_ack,omitempty"`
	Exclusive bool           `msgpack:"exclusive,omitempty"`
	Tag       string         `msgpack:"tag,omitempty"`
	Args      map[string]any `msgpack:"args,omitempty"`
}

type unsubscribeHeader struct {
	Rid uint32 `msgpack:"rid"`
	Sub uint32 `msgpack:"sub"`
}

type creditHeader struct {
	Sub    uint32 `msgpack:"sub"`
	Credit uint32 `msgpack:"credit"`
}

type deliverHeader struct {
	Sub         uint32         `msgpack:"sub"`
	Tag         uint64         `msgpack:"tag"`
	Redelivered bool           `msgpack:"redelivered,omitempty"`
	Exchange    string         `msgpack:"exchange,omitempty"`
	Key         string         `msgpack:"key,omitempty"`
	Props       wireProps      `msgpack:"props,omitempty"`
	Headers     map[string]any `msgpack:"headers,omitempty"`
}

type cancelledHeader struct {
	Sub  uint32 `msgpack:"sub"`
	Code uint16 `msgpack:"code"`
	Msg  string `msgpack:"msg,omitempty"`
}

type ackHeader struct {
	Tag      uint64 `msgpack:"tag"`
	Multiple bool   `msgpack:"multiple,omitempty"`
}

type nackHeader struct {
	Tag      uint64 `msgpack:"tag"`
	Multiple bool   `msgpack:"multiple,omitempty"`
	Requeue  bool   `msgpack:"requeue,omitempty"`
}

type getHeader struct {
	Rid   uint32 `msgpack:"rid"`
	Queue string `msgpack:"queue"`
	NoAck bool   `msgpack:"no_ack,omitempty"`
}

type getOkHeader struct {
	Rid         uint32         `msgpack:"rid"`
	Tag         uint64         `msgpack:"tag"`
	Redelivered bool           `msgpack:"redelivered,omitempty"`
	Exchange    string         `msgpack:"exchange,omitempty"`
	Key         string         `msgpack:"key,omitempty"`
	Remaining   uint32         `msgpack:"remaining"`
	Props       wireProps      `msgpack:"props,omitempty"`
	Headers     map[string]any `msgpack:"headers,omitempty"`
}

// newHeader returns a pointer to the zero header type for op, or nil for ops without
// a header (ping, pong) and unknown ops.
func newHeader(op byte) any {
	switch op {
	case opHello:
		return new(helloHeader)
	case opWelcome:
		return new(welcomeHeader)
	case opClose:
		return new(closeHeader)
	case opOk:
		return new(okHeader)
	case opError:
		return new(errorHeader)
	case opFlow:
		return new(flowHeader)
	case opDeclareExchange:
		return new(declareExchangeHeader)
	case opDeleteExchange:
		return new(deleteExchangeHeader)
	case opDeclareQueue:
		return new(declareQueueHeader)
	case opDeleteQueue:
		return new(deleteQueueHeader)
	case opPurgeQueue:
		return new(purgeQueueHeader)
	case opBind, opUnbind:
		return new(bindHeader)
	case opPublish:
		return new(publishHeader)
	case opConfirm:
		return new(confirmHeader)
	case opReturn:
		return new(returnHeader)
	case opSubscribe:
		return new(subscribeHeader)
	case opUnsubscribe:
		return new(unsubscribeHeader)
	case opCredit:
		return new(creditHeader)
	case opDeliver:
		return new(deliverHeader)
	case opCancelled:
		return new(cancelledHeader)
	case opAck:
		return new(ackHeader)
	case opNack:
		return new(nackHeader)
	case opGet:
		return new(getHeader)
	case opGetOk:
		return new(getOkHeader)
	}
	return nil
}

// decodeCommand decodes a frame's header into its typed form. Ping and pong decode to
// a nil header.
func decodeCommand(f frame) (any, error) {
	h := newHeader(f.op)
	if h == nil {
		if f.op == opPing || f.op == opPong {
			return nil, nil
		}
		return nil, fmt.Errorf("firemq: unknown op %#02x", f.op)
	}
	if err := decodeHeader(f.header, h); err != nil {
		return nil, fmt.Errorf("firemq: invalid %s header: %w", opName(f.op), err)
	}
	return h, nil
}
