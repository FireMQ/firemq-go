// RPC: a request carries reply_to and correlation_id; the server answers on the reply
// queue with the same correlation id.
//
// Start a broker, then run: go run ./examples/rpc
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
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

	if _, err := client.DeclareQueue(ctx, "rpc_queue.go", firemq.QueueOptions{}); err != nil {
		log.Fatal(err)
	}

	// The server squares numbers.
	requests, err := client.Subscribe(ctx, "rpc_queue.go", firemq.SubscribeOptions{})
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for req := range requests.Messages() {
			n, _ := strconv.Atoi(req.BodyString())
			reply := firemq.PublishOptions{
				Properties: firemq.Properties{CorrelationID: req.Properties.CorrelationID},
			}
			err := client.PublishWithOptions(ctx, "", req.Properties.ReplyTo, []byte(strconv.Itoa(n*n)), reply)
			if err != nil {
				log.Print(err)
			}
			req.Ack()
		}
	}()

	// The client sends a request and waits for the matching reply.
	replies, err := client.DeclareQueue(ctx, "", firemq.TemporaryQueue())
	if err != nil {
		log.Fatal(err)
	}
	answers, err := client.Subscribe(ctx, replies.Name, firemq.SubscribeOptions{})
	if err != nil {
		log.Fatal(err)
	}
	request := firemq.PublishOptions{
		Properties: firemq.Properties{ReplyTo: replies.Name, CorrelationID: "req-1"},
	}
	if err := client.PublishWithOptions(ctx, "", "rpc_queue.go", []byte("7"), request); err != nil {
		log.Fatal(err)
	}
	fmt.Println(" [x] Requesting square(7)")
	for {
		answer, err := answers.Next(ctx)
		if err != nil {
			log.Fatal(err)
		}
		answer.Ack()
		if answer.Properties.CorrelationID == "req-1" {
			fmt.Printf(" [.] square(7) = %s\n", answer.Body)
			return
		}
	}
}

func brokerURL() string {
	if url := os.Getenv("FIREMQ_URL"); url != "" {
		return url
	}
	return "fire://localhost"
}
