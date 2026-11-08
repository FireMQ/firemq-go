package firemq

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
)

// Properties are a message's AMQP basic properties. Zero values are not sent.
type Properties struct {
	ContentType     string
	ContentEncoding string
	CorrelationID   string
	ReplyTo         string
	// Expiration is the message's time to live. It travels in whole milliseconds.
	Expiration time.Duration
	MessageID  string
	// Timestamp travels in whole seconds since the Unix epoch.
	Timestamp time.Time
	Type      string
	// UserID must equal the connection's user when set.
	UserID string
	AppID  string
	// Priority ranges from 0 to 255; it only matters on queues with x-max-priority.
	Priority uint8
}

func (p Properties) toWire() wireProps {
	w := wireProps{
		ContentType:     p.ContentType,
		ContentEncoding: p.ContentEncoding,
		CorrelationID:   p.CorrelationID,
		ReplyTo:         p.ReplyTo,
		MessageID:       p.MessageID,
		Type:            p.Type,
		UserID:          p.UserID,
		AppID:           p.AppID,
		Priority:        p.Priority,
	}
	if p.Expiration > 0 {
		w.Expiration = uint64(p.Expiration / time.Millisecond)
	}
	if !p.Timestamp.IsZero() && p.Timestamp.Unix() > 0 {
		w.Timestamp = uint64(p.Timestamp.Unix())
	}
	return w
}

func (w wireProps) toProperties() Properties {
	p := Properties{
		ContentType:     w.ContentType,
		ContentEncoding: w.ContentEncoding,
		CorrelationID:   w.CorrelationID,
		ReplyTo:         w.ReplyTo,
		MessageID:       w.MessageID,
		Type:            w.Type,
		UserID:          w.UserID,
		AppID:           w.AppID,
		Priority:        w.Priority,
	}
	if w.Expiration > 0 {
		p.Expiration = time.Duration(w.Expiration) * time.Millisecond
	}
	if w.Timestamp > 0 {
		p.Timestamp = time.Unix(int64(w.Timestamp), 0).UTC()
	}
	return p
}

// Message is a message received from Subscribe or Get.
type Message struct {
	Body       []byte
	Exchange   string
	RoutingKey string
	Properties Properties
	// Headers are the application headers. Integers decode as int64 or uint64 and
	// floats as float64.
	Headers map[string]any
	// Redelivered is true when the message was delivered before and not acknowledged.
	Redelivered bool
	// DeliveryTag identifies the delivery on its connection.
	DeliveryTag uint64

	acker *acker
}

// acker settles one delivery. Messages that need no acknowledgement have none.
type acker struct {
	client  *Client
	credit  *credit
	settled atomic.Bool
}

// BodyString returns the body as a string.
func (m *Message) BodyString() string { return string(m.Body) }

// JSON decodes a JSON body into v.
func (m *Message) JSON(v any) error {
	if err := json.Unmarshal(m.Body, v); err != nil {
		return fmt.Errorf("firemq: decoding JSON body: %w", err)
	}
	return nil
}

// Ack acknowledges the message. It does nothing for messages received with NoAck or
// when the message was already acknowledged or rejected.
func (m *Message) Ack() error {
	return m.settle(opAck, &ackHeader{Tag: m.DeliveryTag})
}

// Nack rejects the message. With requeue the broker delivers it again; otherwise it is
// dead-lettered when the queue has a dead-letter exchange, and dropped if not. Like Ack,
// it does nothing for NoAck messages or messages already settled.
func (m *Message) Nack(requeue bool) error {
	return m.settle(opNack, &nackHeader{Tag: m.DeliveryTag, Requeue: requeue})
}

func (m *Message) settle(op byte, header any) error {
	a := m.acker
	if a == nil || a.settled.Swap(true) {
		return nil
	}
	if err := a.client.send(op, header, nil); err != nil {
		return err
	}
	if a.credit != nil {
		a.credit.release(1)
	}
	return nil
}
