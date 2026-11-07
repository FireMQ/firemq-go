// Publish/subscribe: a fanout exchange copies every message to every bound queue.
//
// Start a broker, then run: go run ./examples/publish_subscribe
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

	if err := client.DeclareExchange(ctx, "logs.go", "fanout", firemq.ExchangeOptions{}); err != nil {
		log.Fatal(err)
	}

	// Two subscribers, each with its own temporary queue bound to the exchange.
	var subs []*firemq.Subscription
	for i := 0; i < 2; i++ {
		q, err := client.DeclareQueue(ctx, "", firemq.TemporaryQueue())
		if err != nil {
			log.Fatal(err)
		}
		if err := client.BindQueue(ctx, q.Name, "logs.go", "", nil); err != nil {
			log.Fatal(err)
		}
		sub, err := client.Subscribe(ctx, q.Name, firemq.SubscribeOptions{NoAck: true})
		if err != nil {
			log.Fatal(err)
		}
		subs = append(subs, sub)
	}

	// Log messages are not worth keeping across restarts.
	opts := firemq.PublishOptions{Transient: true}
	if err := client.PublishWithOptions(ctx, "logs.go", "", []byte("info: Hello World!"), opts); err != nil {
		log.Fatal(err)
	}
	fmt.Println(" [x] Sent 'info: Hello World!'")

	for i, sub := range subs {
		msg, err := sub.Next(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf(" [subscriber %d] %s\n", i+1, msg.Body)
	}
}

func brokerURL() string {
	if url := os.Getenv("FIREMQ_URL"); url != "" {
		return url
	}
	return "fire://localhost"
}
