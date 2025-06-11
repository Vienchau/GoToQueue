package main

import (
	"context"
	"fmt"
	"time"

	gotoqueue "github.com/your-username/go-to-queue"
)

func main() {
	// Create pool with 3 workers, buffer size 100
	pool := gotoqueue.NewPool(3, 100, gotoqueue.KeyBased)
	pool.Start()
	defer pool.Stop()

	fmt.Println("🚀 Go To Queue - Example Usage")
	fmt.Println("==============================")

	// Basic enqueue
	fmt.Println("\n1. Basic enqueue:")
	pool.Enqueue("user:123", func(ctx context.Context) {
		fmt.Println("   ✅ Processing user 123")
	})

	// With timeout
	fmt.Println("\n2. With timeout:")
	pool.Enqueue("order:456", func(ctx context.Context) {
		fmt.Println("   ✅ Processing order 456")
	}, gotoqueue.WithTimeout(5*time.Second))

	// With expiration
	fmt.Println("\n3. With expiration:")
	pool.Enqueue("task:789", func(ctx context.Context) {
		fmt.Println("   ✅ Processing task 789")
	}, gotoqueue.WithExpirationDuration(10*time.Minute))

	// Multiple items with same key (will be processed sequentially)
	fmt.Println("\n4. Same key sequential processing:")
	for i := 1; i <= 3; i++ {
		id := i
		pool.Enqueue("user:same", func(ctx context.Context) {
			fmt.Printf("   ✅ Processing user:same - task %d\n", id)
			time.Sleep(100 * time.Millisecond) // Simulate work
		})
	}

	// Multiple items with different keys (will be processed concurrently)
	fmt.Println("\n5. Different keys concurrent processing:")
	for i := 1; i <= 3; i++ {
		id := i
		pool.Enqueue(fmt.Sprintf("concurrent:%d", id), func(ctx context.Context) {
			fmt.Printf("   ✅ Processing concurrent:%d\n", id)
			time.Sleep(50 * time.Millisecond) // Simulate work
		})
	}

	// With metadata
	fmt.Println("\n6. With metadata:")
	pool.Enqueue("priority:high", func(ctx context.Context) {
		fmt.Println("   ✅ Processing high priority task")
	}, gotoqueue.WithMetadata(map[string]interface{}{
		"priority": "high",
		"retry":    3,
		"user_id":  12345,
	}))

	// Combined options
	fmt.Println("\n7. Combined options:")
	pool.Enqueue("complex:task", func(ctx context.Context) {
		fmt.Println("   ✅ Processing complex task with all options")
	},
		gotoqueue.WithTimeout(10*time.Second),
		gotoqueue.WithExpirationDuration(5*time.Minute),
		gotoqueue.WithMetadata(map[string]interface{}{
			"priority": "critical",
			"retries":  5,
		}),
	)

	// Wait for all tasks to complete
	time.Sleep(1 * time.Second)

	// Show pool statistics
	fmt.Println("\n📊 Pool Statistics:")
	fmt.Printf("   Pool size: %d workers\n", pool.GetPoolSize())
	fmt.Printf("   Total queued items: %d\n", pool.GetTotalQueueLength())
	fmt.Printf("   Pool running: %v\n", pool.IsRunning())

	fmt.Println("\n🎉 Example completed!")
}
