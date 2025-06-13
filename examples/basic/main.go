package main

import (
	"context"
	"fmt"
	"time"

	gotoqueue "github.com/three-ball/go-to-queue"
)

func processUserinOneSecond(ctx context.Context) {
	userID := ctx.Value("user_id")
	fmt.Printf("User ID: %v\n", userID)
	time.Sleep(time.Second * 3)
	fmt.Printf("User ID: %v processed successfully\n", userID)
}

func main() {
	// Create pool with 3 workers, buffer size 100
	pool := gotoqueue.NewPool(2, 100, gotoqueue.KeyBased)
	pool.SetLogLevel(gotoqueue.LOG_DEBUG) // Enable logging for debugging
	pool.Start()
	defer pool.Stop()

	fmt.Println("🚀 Go To Queue - Example Usage")
	fmt.Println("==============================")

	// Basic enqueue
	fmt.Println("\n1. Basic enqueue:")
	for index := 0; index < 10; index++ {
		id, err := pool.Enqueue(
			fmt.Sprintf("user:%d", index),
			processUserinOneSecond,
			gotoqueue.WithMetadata(
				map[string]interface{}{
					"user_id": fmt.Sprintf("user:%d", index),
				},
			),
			gotoqueue.WithExpirationDuration(5*time.Second), // Set expiration to 30 seconds
		)
		if err != nil {
			fmt.Println("Error enqueuing user:", err)
			continue
		}

		fmt.Printf("Enqueued user: %s with ID: %d\n", fmt.Sprintf("user:%d", index), id)
	}

	fmt.Printf("Enqueue done!")
}
