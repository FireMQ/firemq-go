package firemq

import (
	"crypto/tls"
	"log/slog"
	"time"
)

// Default option values.
const (
	DefaultHeartbeat      = 30 * time.Second
	DefaultConnectTimeout = 10 * time.Second
	DefaultPrefetch       = 100
)

// ConnectOptions configures a connection. The zero value uses the defaults.
type ConnectOptions struct {
	// Heartbeat is the heartbeat interval to ask for, in whole seconds. The broker
	// uses the smaller non-zero value of this and its own setting. Zero means
	// DefaultHeartbeat; a negative value sends 0, which leaves the interval to the
	// broker.
	Heartbeat time.Duration
	// ConnectTimeout bounds the TCP connect, TLS and protocol handshake. Zero means
	// DefaultConnectTimeout. A deadline on the context passed to Connect also applies.
	ConnectTimeout time.Duration
	// ConnectionName is shown in the management UI.
	ConnectionName string

	// TLSConfig is used for fires:// URLs. When nil, the SDK uses the system roots
	// plus CACertPEM. When set, it is cloned and CACertPEM and InsecureSkipVerify are
	// ignored.
	TLSConfig *tls.Config
	// CACertPEM holds extra trusted CA certificates in PEM form, for example a
	// self-signed broker certificate.
	CACertPEM []byte
	// InsecureSkipVerify disables certificate verification. Use it only for local
	// development.
	InsecureSkipVerify bool

	// Logger receives warnings the SDK cannot return to a caller, such as a broker
	// error that is not tied to a request. Nil disables logging.
	Logger *slog.Logger
}

// QueueOptions configures DeclareQueue. The zero value declares a durable queue.
type QueueOptions struct {
	// Transient declares a queue that does not survive a broker restart.
	Transient bool
	// Exclusive restricts the queue to this connection and deletes it when the
	// connection closes.
	Exclusive bool
	// AutoDelete deletes the queue when its last subscription is removed.
	AutoDelete bool
	// Passive only checks that the queue exists.
	Passive bool
	// Args holds RabbitMQ queue arguments, for example "x-message-ttl",
	// "x-max-length" or "x-dead-letter-exchange".
	Args map[string]any
}

// TemporaryQueue returns options for a queue that lives only as long as this
// connection: transient, exclusive and auto-deleted. Pair it with an empty name to
// let the broker pick one.
func TemporaryQueue() QueueOptions {
	return QueueOptions{Transient: true, Exclusive: true, AutoDelete: true}
}

// ExchangeOptions configures DeclareExchange. The zero value declares a durable
// exchange.
type ExchangeOptions struct {
	// Transient declares an exchange that does not survive a broker restart.
	Transient bool
	// AutoDelete deletes the exchange when its last binding is removed.
	AutoDelete bool
	// Internal exchanges cannot be published to directly.
	Internal bool
	// Passive only checks that the exchange exists.
	Passive bool
	// Args holds exchange arguments, for example "alternate-exchange".
	Args map[string]any
}

// PublishOptions configures PublishWithOptions. The zero value publishes a persistent
// message and waits for the broker's confirm.
type PublishOptions struct {
	// Transient messages are kept in memory only.
	Transient bool
	// Mandatory makes the publish fail with *UnroutableError when no queue receives
	// the message. It needs confirms to report the failure.
	Mandatory bool
	// NoConfirm returns as soon as the message is queued for sending, without waiting
	// for the broker's confirm.
	NoConfirm bool
	// Properties are the message's AMQP properties.
	Properties Properties
	// Headers are application headers. Values may be nil, bool, integers, floats,
	// strings, []byte, slices and map[string]any.
	Headers map[string]any
}

// SubscribeOptions configures Subscribe.
type SubscribeOptions struct {
	// Prefetch is how many unacknowledged messages the broker may send at once.
	// Zero means DefaultPrefetch.
	Prefetch uint32
	// NoAck treats messages as acknowledged once sent. Faster, but messages are lost
	// if the consumer crashes.
	NoAck bool
	// Exclusive fails with code 405 when the queue has other consumers, and keeps new
	// ones out.
	Exclusive bool
	// Tag is the consumer tag shown in the management UI.
	Tag string
}
