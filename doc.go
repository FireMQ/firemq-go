// Package firemq is the Go SDK for FireMQ, a RabbitMQ-compatible message broker.
//
// The SDK speaks the FireMQ protocol over TCP (fire://) or TLS (fires://). It has
// the same model as AMQP 0-9-1: exchanges, queues, bindings, acknowledgements and
// publisher confirms.
//
// Defaults favour safety. The zero values of QueueOptions and ExchangeOptions declare
// durable resources, the zero value of PublishOptions publishes a persistent message
// and waits for the broker's confirm, and Subscribe grants the broker 100 messages of
// credit unless told otherwise.
//
// A Client is safe for concurrent use. It runs one goroutine that reads from the
// socket, one that writes to it, and one per active subscription. Close stops all of
// them.
//
// Errors are typed: *ServerError when the broker refuses a request,
// *UnroutableError when a mandatory publish reaches no queue, *NackedError when the
// broker rejects a publish, and ErrClosed (matched with errors.Is) once the connection
// is closed.
//
// The protocol is specified at
// https://github.com/FireMQ/firemq/blob/main/spec/fire-protocol.md.
package firemq
