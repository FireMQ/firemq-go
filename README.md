# firemq-go

The official Go SDK for [FireMQ](https://github.com/FireMQ/firemq), a lean, RabbitMQ-compatible message broker.

The SDK speaks the [FireMQ protocol](https://github.com/FireMQ/firemq/blob/main/spec/fire-protocol.md): length-prefixed frames with MessagePack headers and raw message bodies. It has the same model as AMQP 0-9-1 (exchanges, queues, bindings, acknowledgements and publisher confirms), so a message you publish from Go can be consumed by any AMQP client connected to the same broker, and the other way around.

Its API mirrors the Rust client, `firemq-client`, and passes the FireMQ SDK conformance suite.

## Install

You need Go 1.22 or later.

```bash
go get github.com/FireMQ/firemq-go
```

To try the examples you also need a running broker. FireMQ listens for the FireMQ protocol on port 5673 by default.

## Quickstart

```go
package main

import (
	"context"
	"fmt"
	"log"

	firemq "github.com/FireMQ/firemq-go"
)

func main() {
	ctx := context.Background()

	client, err := firemq.Connect(ctx, "fire://guest:guest@localhost:5673")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(ctx)

	if _, err := client.DeclareQueue(ctx, "jobs", firemq.QueueOptions{}); err != nil {
		log.Fatal(err)
	}

	// Publish returns once the broker has confirmed the message.
	if err := client.Publish(ctx, "", "jobs", []byte("hello")); err != nil {
		log.Fatal(err)
	}

	sub, err := client.Subscribe(ctx, "jobs", firemq.SubscribeOptions{})
	if err != nil {
		log.Fatal(err)
	}
	msg, err := sub.Next(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(msg.BodyString())
	msg.Ack()
}
```

Defaults favour safety: queues and exchanges are durable, messages are persistent, and `Publish` waits for the broker's confirm. The zero value of every options struct gives you these defaults.

## Connect

`Connect` takes a URL:

```text
fire://user:password@host:5673/vhost
fires://user:password@host:5674/vhost
```

Missing parts take these defaults:

| Part | Default |
|---|---|
| User and password | `guest` and `guest` |
| Port | 5673 for `fire://`, 5674 for `fires://` |
| Vhost | `/`. The vhost is the URL path without its leading `/`, percent-decoded, so `/%2F` is also `/`. |

`ConnectWithOptions` accepts a `ConnectOptions` struct:

| Field | Default | Meaning |
|---|---|---|
| `Heartbeat` | 30s | Requested heartbeat interval, in whole seconds. The broker uses the smaller non-zero value of this and its own setting. A negative value leaves the interval to the broker. |
| `ConnectTimeout` | 10s | Bound on the TCP connect, TLS handshake and protocol handshake. A deadline on the context also applies. |
| `ConnectionName` | none | Name shown in the management UI. |
| `TLSConfig`, `CACertPEM`, `InsecureSkipVerify` | system roots | TLS settings for `fires://`. See [TLS](#tls). |
| `Logger` | none | `*slog.Logger` for warnings that have no caller to return to, such as a broker error not tied to a request. |

With heartbeats on, the SDK sends a ping after half the interval without sending anything, and closes the connection when the broker has been silent for twice the interval.

`Close` tells the broker, waits up to two seconds (or until the context ends) for it to acknowledge, and then stops every goroutine the client started. Unacknowledged messages go back to their queues. `Done` returns a channel that is closed when the connection ends for any reason, and `Err` reports why.

A `*Client` is safe for concurrent use. Share one client across goroutines instead of opening a connection per goroutine.

## API overview

### Topology

```go
client.DeclareExchange(ctx, "orders", "topic", firemq.ExchangeOptions{})
info, _ := client.DeclareQueue(ctx, "orders.audit", firemq.QueueOptions{
	Args: map[string]any{"x-message-ttl": 60000},
})
client.BindQueue(ctx, info.Name, "orders", "order.*", nil)
```

| Method | What it does |
|---|---|
| `DeclareQueue(ctx, name, QueueOptions)` | Declares a queue and returns its name, message count and consumer count. An empty name lets the broker pick one. |
| `TemporaryQueue()` | Options for a queue that lives only as long as the connection: transient, exclusive and auto-delete. |
| `DeleteQueue(ctx, name)` | Deletes a queue and returns how many messages it held. |
| `PurgeQueue(ctx, name)` | Removes the ready messages and returns how many it removed. |
| `DeclareExchange(ctx, name, kind, ExchangeOptions)` | Declares a `direct`, `fanout`, `topic` or `headers` exchange. |
| `DeleteExchange(ctx, name, ifUnused)` | Deletes an exchange. |
| `BindQueue(ctx, queue, exchange, key, args)` / `UnbindQueue` | Adds or removes a binding. `args` may be nil. |
| `BindExchange(ctx, destination, source, key)` / `UnbindExchange` | Routes messages from one exchange to another. |

`QueueOptions` and `ExchangeOptions` declare durable resources by default. Set `Transient: true` for one that does not survive a broker restart. Queue arguments use RabbitMQ's names, such as `x-message-ttl`, `x-max-length`, `x-overflow` and `x-dead-letter-exchange`.

### Publish

```go
err := client.PublishWithOptions(ctx, "orders", "order.created", body, firemq.PublishOptions{
	Mandatory:  true,
	Properties: firemq.Properties{ContentType: "application/json", MessageID: "42"},
	Headers:    map[string]any{"attempt": 1},
})
```

- `Publish(ctx, exchange, key, body)` publishes a persistent message and waits for the broker's confirm. Use exchange `""` to send straight to the queue named by `key`.
- `PublishJSON(ctx, exchange, key, v)` encodes `v` as JSON and sets the content type to `application/json`.
- `PublishWithOptions` lets you set `Transient`, `Mandatory`, `NoConfirm`, `Properties` and `Headers`.

The body is copied, so you can reuse the slice once the call returns. Header values can be nil, booleans, integers, floats, strings, `[]byte`, slices and `map[string]any`.

With `NoConfirm`, the call returns as soon as the message is queued for sending, and the broker reports nothing back. A mandatory message that reaches no queue is only reported when confirms are on.

### Consume

```go
sub, err := client.Subscribe(ctx, "jobs", firemq.SubscribeOptions{Prefetch: 10})
if err != nil {
	log.Fatal(err)
}
for msg := range sub.Messages() {
	if err := handle(msg); err != nil {
		msg.Nack(true) // requeue
		continue
	}
	msg.Ack()
}
// The channel closes when the subscription ends; sub.Err() says why.
```

`Subscribe` returns a `*Subscription`. Read messages from the `Messages` channel or call `Next(ctx)`. Each `*Message` carries `Body`, `Exchange`, `RoutingKey`, `Properties`, `Headers`, `Redelivered` and `DeliveryTag`, plus `BodyString()` and `JSON(v)`.

`SubscribeOptions`:

| Field | Default | Meaning |
|---|---|---|
| `Prefetch` | 100 | How many unacknowledged messages the broker may send at once. |
| `NoAck` | false | Treat messages as acknowledged once sent. Faster, but messages are lost if the consumer crashes. |
| `Exclusive` | false | Fail with code 405 when the queue has other consumers, and keep new ones out. |
| `Tag` | `fire-<id>` | Consumer tag shown in the management UI. |

Flow control uses credit. The SDK grants `Prefetch` credit when it subscribes and grants more in batches of half that window as you call `Ack` or `Nack`. With `NoAck`, it grants credit as you read messages. If you stop acknowledging, the broker stops sending after `Prefetch` messages.

`Cancel(ctx)` ends the subscription and waits for the broker to confirm. Messages the SDK had received but you had not read yet go back to the queue. Messages you already hold can still be acknowledged.

`Get(ctx, queue, noAck)` takes a single message, or returns `nil` when the queue is empty. Without `noAck`, acknowledge it with `Ack` or `Nack`.

`Ack` and `Nack` do nothing for messages received with `NoAck`, or for a message you have already settled.

## Errors

Every operation returns an error you can inspect with `errors.As` and `errors.Is`:

| Error | When |
|---|---|
| `*firemq.ServerError` | The broker refused a request. `Code` is an AMQP reply code, for example 403 (access refused), 404 (not found) or 406 (precondition failed). |
| `*firemq.UnroutableError` | A mandatory publish reached no queue. |
| `*firemq.NackedError` | The broker rejected a publish, for example because a queue with `x-overflow: reject-publish` is full. |
| `firemq.ErrClosed` | The connection is closed. When a broker error closed it, `errors.As` also finds the `*ServerError`. |
| `firemq.ErrCancelled` | Returned by `Subscription.Next` after the subscription was cancelled by you or by the broker. |

```go
err := client.PublishWithOptions(ctx, "orders", "order.unknown", body, firemq.PublishOptions{Mandatory: true})
var unroutable *firemq.UnroutableError
var server *firemq.ServerError
switch {
case errors.As(err, &unroutable):
	log.Printf("no queue for %s", unroutable.RoutingKey)
case errors.As(err, &server) && server.Code == 404:
	log.Print("the exchange does not exist")
case errors.Is(err, firemq.ErrClosed):
	log.Print("reconnect and retry")
}
```

`firemq.ErrorCode(err)` returns the reply code carried by any of these errors, or 0.

A refused request does not close the connection. Only fatal errors, such as a malformed frame, do. The SDK does not reconnect automatically; watch `client.Done()` if you want to reconnect.

## TLS

Use a `fires://` URL. By default the SDK verifies the broker's certificate against the system roots.

To trust a private or self-signed CA, pass its certificate in PEM form:

```go
ca, err := os.ReadFile("ca.pem")
if err != nil {
	log.Fatal(err)
}
client, err := firemq.ConnectWithOptions(ctx, "fires://mq.example.com", firemq.ConnectOptions{
	CACertPEM: ca,
})
```

For full control, such as client certificates, set `TLSConfig`. The SDK clones it and fills in `ServerName` from the URL when it's empty. `CACertPEM` and `InsecureSkipVerify` are ignored when `TLSConfig` is set.

`InsecureSkipVerify: true` turns off certificate verification. Use it only for local development.

## Examples

The [`examples`](examples) directory has a runnable program for each RabbitMQ tutorial pattern: `hello_world`, `work_queues`, `publish_subscribe`, `routing`, `topics` and `rpc`. Each one publishes, consumes and exits. They connect to `fire://localhost` unless you set `FIREMQ_URL`:

```bash
FIREMQ_URL=fire://localhost:5673 go run ./examples/hello_world
```

## Tests

```bash
go test -race ./...
```

The unit tests cover the codec, including every frame in the protocol's test vectors, and the client against an in-process fake broker. Tests that need a real broker run when `FIREMQ_URL` is set.

The [conformance suite](https://github.com/FireMQ/firemq/blob/main/spec/conformance.md) drives the SDK through `conformance/driver`:

```bash
firemq-conformance run --driver "go run ./conformance/driver"
```

## Not supported yet

- WebSocket transport (`ws://` and `wss://`). Use `fire://` or `fires://`.
- Automatic reconnection.
- Pausing publishers on the broker's `flow` frame. The broker throttles a client that keeps publishing during a memory or disk alarm by not reading its socket.

## Learn more

- [FireMQ documentation](https://github.com/FireMQ/firemq/tree/main/docs)
- [FireMQ protocol specification](https://github.com/FireMQ/firemq/blob/main/spec/fire-protocol.md)
- [API reference](https://pkg.go.dev/github.com/FireMQ/firemq-go)

## License

Apache-2.0. See [LICENSE](LICENSE).
