// Topics: route with word patterns. "*" matches exactly one word and "#" matches zero
// or more.
//
// Start a broker, then run: go run ./examples/topics
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

	if err := client.DeclareExchange(ctx, "topic_logs.go", "topic", firemq.ExchangeOptions{}); err != nil {
		log.Fatal(err)
	}
	q, err := client.DeclareQueue(ctx, "", firemq.TemporaryQueue())
	if err != nil {
		log.Fatal(err)
	}
	if err := client.BindQueue(ctx, q.Name, "topic_logs.go", "*.critical", nil); err != nil {
		log.Fatal(err)
	}

	for _, key := range []string{"kern.critical", "kern.info", "app.critical"} {
		if err := client.Publish(ctx, "topic_logs.go", key, []byte(key)); err != nil {
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
		fmt.Printf(" [x] %s\n", msg.Body)
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
