// Work queues: share tasks between workers, one unacknowledged task per worker at a
// time.
//
// Start a broker, then run: go run ./examples/work_queues
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	firemq "github.com/FireMQ/firemq-go"
)

const tasks = 6

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := firemq.Connect(ctx, brokerURL())
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(context.Background())

	if _, err := client.DeclareQueue(ctx, "task_queue.go", firemq.QueueOptions{}); err != nil {
		log.Fatal(err)
	}
	for n := 1; n <= tasks; n++ {
		// Persistent by default: returns once the broker has written the task to disk.
		if err := client.Publish(ctx, "", "task_queue.go", []byte(fmt.Sprintf("task %d", n))); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf(" [x] Sent %d tasks\n", tasks)

	// Two workers. Prefetch 1 means the broker sends a worker its next task only
	// after it acknowledged the previous one.
	var wg sync.WaitGroup
	var mu sync.Mutex
	done := 0
	finished := make(chan struct{})
	for w := 1; w <= 2; w++ {
		sub, err := client.Subscribe(ctx, "task_queue.go", firemq.SubscribeOptions{Prefetch: 1})
		if err != nil {
			log.Fatal(err)
		}
		wg.Add(1)
		go func(worker int, sub *firemq.Subscription) {
			defer wg.Done()
			for {
				select {
				case msg, ok := <-sub.Messages():
					if !ok {
						return
					}
					time.Sleep(50 * time.Millisecond) // pretend to work
					fmt.Printf(" [worker %d] Done %s\n", worker, msg.Body)
					if err := msg.Ack(); err != nil {
						log.Fatal(err)
					}
					mu.Lock()
					done++
					if done == tasks {
						close(finished)
					}
					mu.Unlock()
				case <-finished:
					return
				}
			}
		}(w, sub)
	}

	select {
	case <-finished:
	case <-ctx.Done():
		log.Fatal("timed out waiting for the workers")
	}
	wg.Wait()
}

func brokerURL() string {
	if url := os.Getenv("FIREMQ_URL"); url != "" {
		return url
	}
	return "fire://localhost"
}
