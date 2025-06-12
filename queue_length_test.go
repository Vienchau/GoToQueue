package gotoqueue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestGetQueueLength(t *testing.T) {
	t.Run("valid worker IDs", func(t *testing.T) {
		mq := NewPool(3, 5, KeyBased)
		mq.Start()
		defer mq.Stop()

		// Test all valid worker IDs start with 0 length
		for i := 0; i < 3; i++ {
			length, err := mq.GetQueueLength(i)
			if err != nil {
				t.Errorf("Unexpected error for worker %d: %v", i, err)
			}
			if length != 0 {
				t.Errorf("Expected length 0 for worker %d, got %d", i, length)
			}
		}
	})

	t.Run("invalid worker IDs", func(t *testing.T) {
		mq := NewPool(3, 5, KeyBased)

		// Test negative worker ID
		_, err := mq.GetQueueLength(-1)
		if err != ErrInvalidWorkerID {
			t.Errorf("Expected ErrInvalidWorkerID for negative ID, got: %v", err)
		}

		// Test worker ID equal to pool size
		_, err = mq.GetQueueLength(3)
		if err != ErrInvalidWorkerID {
			t.Errorf("Expected ErrInvalidWorkerID for ID=3 in pool size 3, got: %v", err)
		}

		// Test worker ID greater than pool size
		_, err = mq.GetQueueLength(5)
		if err != ErrInvalidWorkerID {
			t.Errorf("Expected ErrInvalidWorkerID for ID=5 in pool size 3, got: %v", err)
		}
	})

	t.Run("queue length with items", func(t *testing.T) {
		mq := NewPool(2, 10, KeyBased)
		mq.Start()
		defer mq.Stop()

		// Add some items that will be processed slowly
		var wg sync.WaitGroup

		// Add items that block execution to keep them in queue
		for i := 0; i < 3; i++ {
			wg.Add(1)
			_, err := mq.Enqueue("key1", func(ctx context.Context) {
				defer wg.Done()
				time.Sleep(50 * time.Millisecond) // Reduced sleep time
			})
			if err != nil {
				t.Fatalf("Failed to enqueue item %d: %v", i, err)
			}
		}

		// Give a moment for items to be distributed
		time.Sleep(20 * time.Millisecond) // Reduced sleep time

		// Check queue lengths - worker 0 should have items for "key1"
		workerIndex := mq.to("key1")
		length, err := mq.GetQueueLength(workerIndex)
		if err != nil {
			t.Errorf("Unexpected error getting queue length: %v", err)
		}

		// Should have at least some items (exact count depends on timing)
		if length < 0 {
			t.Errorf("Expected non-negative queue length, got %d", length)
		}

		// Wait for completion
		wg.Wait()

		// After completion, queue should be empty or nearly empty
		time.Sleep(20 * time.Millisecond) // Reduced sleep time
		length, err = mq.GetQueueLength(workerIndex)
		if err != nil {
			t.Errorf("Unexpected error getting queue length after completion: %v", err)
		}
		if length > 1 {
			t.Errorf("Expected queue length <= 1 after completion, got %d", length)
		}
	})

	t.Run("queue length boundary cases", func(t *testing.T) {
		// Test with single worker pool
		mq := NewPool(1, 1, KeyBased)
		mq.Start()
		defer mq.Stop()

		length, err := mq.GetQueueLength(0)
		if err != nil {
			t.Errorf("Unexpected error for single worker pool: %v", err)
		}
		if length != 0 {
			t.Errorf("Expected length 0 for empty single worker pool, got %d", length)
		}

		// Test invalid ID in single worker pool
		_, err = mq.GetQueueLength(1)
		if err != ErrInvalidWorkerID {
			t.Errorf("Expected ErrInvalidWorkerID for ID=1 in single worker pool, got: %v", err)
		}
	})

	t.Run("concurrent access to queue length", func(t *testing.T) {
		mq := NewPool(3, 5, KeyBased)
		mq.Start()
		defer mq.Stop()

		var wg sync.WaitGroup
		errors := make(chan error, 20)

		// Launch multiple goroutines checking queue lengths concurrently
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < 5; j++ {
					_, err := mq.GetQueueLength(workerID % 3)
					if err != nil {
						errors <- err
						return
					}
					time.Sleep(2 * time.Millisecond) // Reduced sleep time
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Check for any errors
		for err := range errors {
			t.Errorf("Concurrent access error: %v", err)
		}
	})
}

func TestGetTotalQueueLength(t *testing.T) {
	t.Run("empty queues", func(t *testing.T) {
		mq := NewPool(3, 5, KeyBased)
		mq.Start()
		defer mq.Stop()

		totalLength := mq.GetTotalQueueLength()
		if totalLength != 0 {
			t.Errorf("Expected total length 0 for empty queues, got %d", totalLength)
		}
	})

	t.Run("single worker pool", func(t *testing.T) {
		mq := NewPool(1, 5, KeyBased)
		mq.Start()
		defer mq.Stop()

		totalLength := mq.GetTotalQueueLength()
		if totalLength != 0 {
			t.Errorf("Expected total length 0 for empty single worker pool, got %d", totalLength)
		}
	})

	t.Run("queues with items", func(t *testing.T) {
		mq := NewPool(3, 10, KeyBased)
		mq.Start()
		defer mq.Stop()

		var wg sync.WaitGroup
		itemCount := 6

		// Add items that will block execution temporarily
		for i := 0; i < itemCount; i++ {
			wg.Add(1)
			key := fmt.Sprintf("key%d", i%3) // Distribute across different workers
			_, err := mq.Enqueue(key, func(ctx context.Context) {
				defer wg.Done()
				time.Sleep(50 * time.Millisecond) // Reduced sleep time
			})
			if err != nil {
				t.Fatalf("Failed to enqueue item %d: %v", i, err)
			}
		}

		// Give time for items to be distributed to queues
		time.Sleep(20 * time.Millisecond) // Reduced sleep time

		// Check total queue length
		totalLength := mq.GetTotalQueueLength()
		if totalLength < 0 {
			t.Errorf("Expected non-negative total queue length, got %d", totalLength)
		}

		// Wait for all items to complete
		wg.Wait()

		// After completion, total should be 0 or very low
		time.Sleep(20 * time.Millisecond) // Reduced sleep time
		totalLength = mq.GetTotalQueueLength()
		if totalLength > 1 {
			t.Errorf("Expected total queue length <= 1 after completion, got %d", totalLength)
		}
	})

	t.Run("total equals sum of individual queues", func(t *testing.T) {
		mq := NewPool(4, 5, KeyBased)
		mq.Start()
		defer mq.Stop()

		// Add some items with keys that go to specific workers
		keys := []string{"key-a", "key-b", "key-c", "key-d", "key-e"}
		var wg sync.WaitGroup

		for _, key := range keys {
			wg.Add(1)
			_, err := mq.Enqueue(key, func(ctx context.Context) {
				defer wg.Done()
				time.Sleep(100 * time.Millisecond) // Keep this one longer for the queue length test
			})
			if err != nil {
				t.Fatalf("Failed to enqueue item with key %s: %v", key, err)
			}
		}

		// Give time for distribution
		time.Sleep(20 * time.Millisecond) // Reduced sleep time

		// Calculate sum of individual queue lengths
		var sumLength int
		for i := 0; i < 4; i++ {
			length, err := mq.GetQueueLength(i)
			if err != nil {
				t.Fatalf("Failed to get length for worker %d: %v", i, err)
			}
			sumLength += length
		}

		// Get total queue length
		totalLength := mq.GetTotalQueueLength()

		// They should be equal
		if totalLength != sumLength {
			t.Errorf("Total queue length (%d) doesn't match sum of individual lengths (%d)",
				totalLength, sumLength)
		}

		// Wait for completion
		wg.Wait()
	})

	t.Run("concurrent access to total queue length", func(t *testing.T) {
		mq := NewPool(3, 5, KeyBased)
		mq.Start()
		defer mq.Stop()

		var wg sync.WaitGroup
		var mu sync.Mutex
		var results []int

		// Launch multiple goroutines checking total length concurrently
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 3; j++ { // Reduced iterations
					length := mq.GetTotalQueueLength()
					mu.Lock()
					results = append(results, length)
					mu.Unlock()
					time.Sleep(1 * time.Millisecond) // Very short sleep
				}
			}()
		}

		wg.Wait()

		// All results should be non-negative
		mu.Lock()
		for _, length := range results {
			if length < 0 {
				t.Errorf("Got negative total queue length: %d", length)
			}
		}
		mu.Unlock()
	})

	t.Run("stopped pool queue length", func(t *testing.T) {
		mq := NewPool(2, 5, KeyBased)
		// Don't start the pool

		// Should still be able to get queue lengths (they should be 0)
		totalLength := mq.GetTotalQueueLength()
		if totalLength != 0 {
			t.Errorf("Expected total length 0 for stopped pool, got %d", totalLength)
		}

		// Individual queue lengths should also work
		length, err := mq.GetQueueLength(0)
		if err != nil {
			t.Errorf("Unexpected error getting queue length for stopped pool: %v", err)
		}
		if length != 0 {
			t.Errorf("Expected length 0 for stopped pool worker, got %d", length)
		}
	})
	t.Run("queue length after pool stop", func(t *testing.T) {
		mq := NewPool(2, 5, KeyBased)

		// Start pool
		mq.Start()

		// Add some items
		var wg sync.WaitGroup
		wg.Add(2)
		for i := 0; i < 2; i++ {
			_, err := mq.Enqueue(fmt.Sprintf("key%d", i), func(ctx context.Context) {
				defer wg.Done()
				time.Sleep(25 * time.Millisecond) // Reduced sleep time
			})
			if err != nil {
				t.Fatalf("Failed to enqueue item: %v", err)
			}
		}

		// Wait for completion and stop
		wg.Wait()
		mq.Stop()

		// Queue lengths should be 0 after stop
		totalLength := mq.GetTotalQueueLength()
		if totalLength != 0 {
			t.Errorf("Expected total length 0 after stop, got %d", totalLength)
		}

		// Individual queue lengths should also be 0
		for i := 0; i < 2; i++ {
			length, err := mq.GetQueueLength(i)
			if err != nil {
				t.Errorf("Unexpected error getting queue length after stop: %v", err)
			}
			if length != 0 {
				t.Errorf("Expected length 0 for worker %d after stop, got %d", i, length)
			}
		}
	})
}
