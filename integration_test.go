package firemq_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	firemq "github.com/FireMQ/firemq-go"
)

// These tests need a running broker. Set FIREMQ_URL, for example
// FIREMQ_URL=fire://localhost:5673, to run them.
func connect(t *testing.T) (*firemq.Client, context.Context) {
	t.Helper()
	url := os.Getenv("FIREMQ_URL")
	if url == "" {
		t.Skip("FIREMQ_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	c, err := firemq.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(context.Background()) })
	return c, ctx
}

func TestIntegrationPublishSubscribe(t *testing.T) {
	c, ctx := connect(t)
	// Not auto-delete, so the queue outlives the subscription below.
	q, err := c.DeclareQueue(ctx, "", firemq.QueueOptions{Transient: true, Exclusive: true})
	if err != nil {
		t.Fatal(err)
	}
	props := firemq.Properties{
		ContentType:   "text/plain",
		CorrelationID: "corr",
		Expiration:    time.Minute,
		Timestamp:     time.Unix(1767225600, 0).UTC(),
		Priority:      3,
	}
	headers := map[string]any{"n": 1, "s": "x", "f": 1.5, "b": true, "list": []any{1, "two"}}
	for i := 0; i < 10; i++ {
		err := c.PublishWithOptions(ctx, "", q.Name, []byte(fmt.Sprint(i)), firemq.PublishOptions{Properties: props, Headers: headers})
		if err != nil {
			t.Fatal(err)
		}
	}
	sub, err := c.Subscribe(ctx, q.Name, firemq.SubscribeOptions{Prefetch: 4})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		m, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if m.BodyString() != fmt.Sprint(i) || m.RoutingKey != q.Name || m.Properties != props {
			t.Fatalf("message %d = %q %q %+v", i, m.Body, m.RoutingKey, m.Properties)
		}
		if m.Headers["n"] != int64(1) || m.Headers["f"] != 1.5 || m.Headers["b"] != true {
			t.Fatalf("headers = %#v", m.Headers)
		}
		if err := m.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	if err := sub.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	if m, err := c.Get(ctx, q.Name, false); err != nil || m != nil {
		t.Fatalf("Get = %v, %v; want an empty queue", m, err)
	}
}

func TestIntegrationErrors(t *testing.T) {
	c, ctx := connect(t)
	err := c.PublishWithOptions(ctx, "amq.direct", "nowhere.go", []byte("x"), firemq.PublishOptions{Mandatory: true})
	var ue *firemq.UnroutableError
	if !errors.As(err, &ue) {
		t.Fatalf("mandatory publish: %v", err)
	}
	err = c.BindQueue(ctx, "missing.go", "missing.go", "", nil)
	var se *firemq.ServerError
	if !errors.As(err, &se) || se.Code != 404 {
		t.Fatalf("bind: %v", err)
	}
	// The connection stays usable.
	if _, err := c.DeclareQueue(ctx, "", firemq.TemporaryQueue()); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationCloseStopsGoroutines(t *testing.T) {
	if os.Getenv("FIREMQ_URL") == "" {
		t.Skip("FIREMQ_URL is not set")
	}
	before := runtime.NumGoroutine()
	ctx := context.Background()
	c, err := firemq.Connect(ctx, os.Getenv("FIREMQ_URL"))
	if err != nil {
		t.Fatal(err)
	}
	q, err := c.DeclareQueue(ctx, "", firemq.TemporaryQueue())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Subscribe(ctx, q.Name, firemq.SubscribeOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := c.Publish(ctx, "", q.Name, []byte("unread")); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(ctx, "", q.Name, nil); !errors.Is(err, firemq.ErrClosed) {
		t.Fatalf("publish after Close: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines before, %d after Close", before, runtime.NumGoroutine())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestIntegrationHeartbeatsKeepIdleConnectionsOpen(t *testing.T) {
	url := os.Getenv("FIREMQ_URL")
	if url == "" {
		t.Skip("FIREMQ_URL is not set")
	}
	ctx := context.Background()
	c, err := firemq.ConnectWithOptions(ctx, url, firemq.ConnectOptions{Heartbeat: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	if hb := c.Server().Heartbeat; hb != time.Second {
		t.Fatalf("negotiated heartbeat = %s, want 1s", hb)
	}
	time.Sleep(3500 * time.Millisecond) // well past 2x the interval
	if _, err := c.DeclareQueue(ctx, "", firemq.TemporaryQueue()); err != nil {
		t.Fatalf("after idling: %v", err)
	}
}
