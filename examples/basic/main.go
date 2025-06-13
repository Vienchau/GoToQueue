package main

import (
	"context"
	"fmt"
	"time"

	gotoqueue "github.com/three-ball/go-to-queue"
)

func processUserinOneSecond(ctx context.Context) {
	var userID interface{}
	var ok bool
	if userID, ok = gotoqueue.GetMetadata(ctx, "user_id"); ok {
		fmt.Printf("User ID: %s\n", userID)
	}

	time.Sleep(time.Second * 1)
	fmt.Printf("User ID: %v processed successfully\n", userID)
}

func processUserWithContextAwareness(ctx context.Context) {
	var userID interface{}
	var ok bool
	if userID, ok = gotoqueue.GetMetadata(ctx, "user_id"); ok {
		fmt.Printf("User ID: %s\n", userID)
	}

	// Approach 1: Use select with timer for context-aware sleep
	select {
	case <-time.After(5 * time.Second):
		// Work completed normally
		fmt.Printf("Processing user ID: %v\n", userID)
		fmt.Printf("User ID: %v processed successfully\n", userID)
	case <-ctx.Done():
		// Context was cancelled/timed out
		fmt.Printf("Function cancelled due to context: %v\n", ctx.Err())
		return
	}
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
	for index := 0; index < 2; index++ {
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

	// Enqueue with context awareness
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fmt.Println("\n2. Enqueue with context awareness:")
	pool.Enqueue(
		"user:context_aware",
		processUserWithContextAwareness,
		gotoqueue.WithContext(ctx),
		gotoqueue.WithMetadata(
			map[string]interface{}{
				"user_id": "user:context_aware",
			},
		),
	)
	fmt.Printf("Enqueued user with context awareness: %s\n", "user:context_aware")
	time.Sleep(10 * time.Second) // Wait for the context to expire
}
