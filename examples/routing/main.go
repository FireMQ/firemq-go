// Routing: a direct exchange delivers a message to the queues bound with its exact
// routing key.
//
// Start a broker, then run: go run ./examples/routing
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	firemq "github.com/FireMQ/firemq-go"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := firemq.Connect(ctx, brokerURL())
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(context.Background())

	if err := client.DeclareExchange(ctx, "direct_logs.go", "direct", firemq.ExchangeOptions{}); err != nil {
		log.Fatal(err)
	}
	// This consumer only cares about warnings and errors.
	q, err := client.DeclareQueue(ctx, "", firemq.TemporaryQueue())
	if err != nil {
		log.Fatal(err)
	}
	for _, severity := range []string{"warning", "error"} {
		if err := client.BindQueue(ctx, q.Name, "direct_logs.go", severity, nil); err != nil {
			log.Fatal(err)
		}
	}

	logs := []struct{ severity, text string }{
		{"info", "service started"},
		{"warning", "disk 90% full"},
		{"error", "cannot reach the database"},
	}
	for _, l := range logs {
		if err := client.Publish(ctx, "direct_logs.go", l.severity, []byte(l.text)); err != nil {
			log.Fatal(err)
		}
	}

	sub, err := client.Subscribe(ctx, q.Name, firemq.SubscribeOptions{})
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		msg, err := sub.Next(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf(" [x] %s: %s\n", msg.RoutingKey, msg.Body)
		if err := msg.Ack(); err != nil {
			log.Fatal(err)
		}
	}
}

func brokerURL() string {
	if url := os.Getenv("FIREMQ_URL"); url != "" {
		return url
	}
	return "fire://localhost"
}
