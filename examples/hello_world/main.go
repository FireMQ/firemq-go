// Hello world: publish one message to a queue and consume it.
//
// Start a broker, then run: go run ./examples/hello_world
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

	if _, err := client.DeclareQueue(ctx, "hello.go", firemq.QueueOptions{}); err != nil {
		log.Fatal(err)
	}
	// Publish returns once the broker has confirmed the message.
	if err := client.Publish(ctx, "", "hello.go", []byte("Hello World!")); err != nil {
		log.Fatal(err)
	}
	fmt.Println(" [x] Sent 'Hello World!'")

	sub, err := client.Subscribe(ctx, "hello.go", firemq.SubscribeOptions{})
	if err != nil {
		log.Fatal(err)
	}
	msg, err := sub.Next(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(" [x] Received '%s'\n", msg.Body)
	if err := msg.Ack(); err != nil {
		log.Fatal(err)
	}
}

func brokerURL() string {
	if url := os.Getenv("FIREMQ_URL"); url != "" {
		return url
	}
	return "fire://localhost"
}
