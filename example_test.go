package firemq_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	firemq "github.com/FireMQ/firemq-go"
)

func Example() {
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

func ExampleClient_Subscribe() {
	ctx := context.Background()
	client, err := firemq.Connect(ctx, "fire://localhost")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(ctx)

	sub, err := client.Subscribe(ctx, "jobs", firemq.SubscribeOptions{Prefetch: 10})
	if err != nil {
		log.Fatal(err)
	}
	for msg := range sub.Messages() {
		fmt.Printf("%s: %s\n", msg.RoutingKey, msg.Body)
		msg.Ack()
	}
	// The channel closes when the subscription ends.
	log.Print(sub.Err())
}

func ExampleClient_PublishWithOptions() {
	ctx := context.Background()
	client, err := firemq.Connect(ctx, "fire://localhost")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(ctx)

	err = client.PublishWithOptions(ctx, "orders", "order.created", []byte(`{"id":42}`), firemq.PublishOptions{
		Mandatory:  true,
		Properties: firemq.Properties{ContentType: "application/json", MessageID: "42"},
		Headers:    map[string]any{"attempt": 1},
	})
	var unroutable *firemq.UnroutableError
	switch {
	case errors.As(err, &unroutable):
		log.Printf("no queue is bound for %s", unroutable.RoutingKey)
	case err != nil:
		log.Fatal(err)
	}
}

func ExampleConnectWithOptions() {
	ca, err := os.ReadFile("ca.pem")
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	client, err := firemq.ConnectWithOptions(ctx, "fires://mq.example.com", firemq.ConnectOptions{
		CACertPEM:      ca,
		ConnectionName: "billing-worker",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(ctx)
}
