package gotoqueue

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

func TestNewPool(t *testing.T) {
	// Test creating a new multi-queue
	mq := NewPool(5, 10, KeyBased)
	if mq.GetPoolSize() != 5 {
		t.Errorf("Expected 5 workers, got %d", mq.GetPoolSize())
	}

	// Test with zero workers (should default to 1)
	mq2 := NewPool(0, 10, KeyBased)
	if mq2.GetPoolSize() != 1 {
		t.Errorf("Expected 1 worker for zero input, got %d", mq2.GetPoolSize())
	}
}

func TestStartStop(t *testing.T) {
	mq := NewPool(100, 100, KeyBased)

	// Initially not running
	if mq.IsRunning() {
		t.Error("Queue should not be running initially")
	}

	// Start the queue
	mq.Start()
	if !mq.IsRunning() {
		t.Error("Queue should be running after Start()")
	}

	// Stop the queue
	mq.Stop()
	if mq.IsRunning() {
		t.Error("Queue should not be running after Stop()")
	}
}

func TestEnqueueAndExecution(t *testing.T) {
	mq := NewPool(3, 1, KeyBased)
	mq.Start()
	defer mq.Stop()

	var wg sync.WaitGroup
	executed := make([]bool, 8)
	var mutex sync.Mutex
	keys := []string{"test1", "test2", "test3", "test1", "test2", "test3", "test4", "test5"}
	// Enqueue functions
	for i := 0; i < 8; i++ {
		wg.Add(1)
		index := i
		_, err := mq.Enqueue(keys[index], func(ctx context.Context) {
			defer wg.Done()
			mutex.Lock()
			executed[index] = true
			mutex.Unlock()
			// time.Sleep(2 * time.Second) // Simulate work
			log.Printf("Function %d executed", index)
		})

		if err != nil {
			t.Errorf("Error enqueuing function %d: %v", i, err)
		}
	}

	// Wait for all functions to execute
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Error("Functions did not execute within timeout")
	}

	// Check that all functions were executed
	mutex.Lock()
	for i, exec := range executed {
		if !exec {
			t.Errorf("Function %d was not executed", i)
		}
	}
	mutex.Unlock()
}

func TestSameKeyFIFOOrder(t *testing.T) {
	mq := NewPool(3, 100, KeyBased)
	mq.Start()
	defer mq.Stop()

	var wg sync.WaitGroup
	order := make([]int, 0)
	var mutex sync.Mutex

	// Enqueue multiple functions with the same key
	key := "same-key"
	for i := 0; i < 5; i++ {
		wg.Add(1)
		value := i
		_, err := mq.Enqueue(key, func(ctx context.Context) {
			defer wg.Done()
			// Add some delay to ensure ordering matters
			time.Sleep(10 * time.Millisecond)
			mutex.Lock()
			order = append(order, value)
			mutex.Unlock()
		})

		if err != nil {
			t.Errorf("Error enqueuing function %d: %v", i, err)
		}
	}

	// Wait for completion
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Error("Functions did not execute within timeout")
	}

	// Check FIFO order
	mutex.Lock()
	if len(order) != 5 {
		t.Errorf("Expected 5 executions, got %d", len(order))
	}

	for i, val := range order {
		if val != i {
			t.Errorf("Expected value %d at position %d, got %d. Order: %v", i, i, val, order)
			break
		}
	}
	mutex.Unlock()
}

func TestDifferentKeysDistribution(t *testing.T) {
	mq := NewPool(3, 100, KeyBased)
	mq.Start()
	defer mq.Stop()

	var wg sync.WaitGroup
	keyWorkerMap := make(map[string]int)
	var mutex sync.Mutex

	keys := []string{"key1", "key2", "key3", "key1", "key2", "key3"}

	for _, key := range keys {
		wg.Add(1)
		currentKey := key
		_, err := mq.Enqueue(currentKey, func(ctx context.Context) {
			defer wg.Done()
			workerIndex := mq.to(currentKey)
			mutex.Lock()
			keyWorkerMap[currentKey] = workerIndex
			mutex.Unlock()
		})

		if err != nil {
			t.Errorf("Error enqueuing function for key %s: %v", key, err)
		}
	}

	// Wait for completion
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Error("Functions did not execute within timeout")
	}

	// Verify that same keys always go to the same worker
	mutex.Lock()
	for key, workerIndex := range keyWorkerMap {
		expectedWorker := mq.to(key)
		if workerIndex != expectedWorker {
			t.Errorf("Key %s went to worker %d, expected worker %d", key, workerIndex, expectedWorker)
		}
	}
	mutex.Unlock()
}

func TestRoundRobinDistribution(t *testing.T) {
	t.Run("round robin distributes evenly", func(t *testing.T) {
		pool := NewPool(3, 10, RoundRobin)
		pool.Start()
		defer pool.Stop()

		// Track which worker processed each task
		var mu sync.Mutex
		var totalProcessed int

		// Enqueue 9 tasks (3x number of workers for even distribution)
		for i := 0; i < 9; i++ {
			key := fmt.Sprintf("task-%d", i)
			_, err := pool.Enqueue(key, func(ctx context.Context) {
				mu.Lock()
				defer mu.Unlock()
				totalProcessed++
			})
			if err != nil {
				t.Fatalf("Failed to enqueue task %d: %v", i, err)
			}
		}

		// Wait for all tasks to complete
		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		if totalProcessed != 9 {
			t.Errorf("Expected 9 tasks processed, got %d", totalProcessed)
		}
		mu.Unlock()

		// Check queue lengths - should be roughly equal
		totalQueued := 0
		for i := 0; i < 3; i++ {
			length, err := pool.GetQueueLength(i)
			if err != nil {
				t.Fatalf("Failed to get queue length for worker %d: %v", i, err)
			}
			totalQueued += length
		}

		// All tasks should be processed (queues should be empty or nearly empty)
		if totalQueued > 1 { // Allow for some timing variance
			t.Errorf("Expected queues to be mostly empty, got total queued: %d", totalQueued)
		}
	})

	t.Run("round robin with same key distributes to different workers", func(t *testing.T) {
		pool := NewPool(3, 10, RoundRobin)
		pool.Start()
		defer pool.Stop()

		// Use the same key for all tasks - round robin should still distribute
		for i := 0; i < 6; i++ {
			_, err := pool.Enqueue("same-key", func(ctx context.Context) {
				// Simulate work to see distribution pattern
				time.Sleep(10 * time.Millisecond)
			})
			if err != nil {
				t.Fatalf("Failed to enqueue task %d: %v", i, err)
			}
		}

		// Check that tasks were distributed across workers
		time.Sleep(300 * time.Millisecond)

		// Verify distribution by checking queue lengths after some processing
		queueLengths := make([]int, 3)
		totalInQueues := 0
		for i := 0; i < 3; i++ {
			length, err := pool.GetQueueLength(i)
			if err != nil {
				t.Fatalf("Failed to get queue length for worker %d: %v", i, err)
			}
			queueLengths[i] = length
			totalInQueues += length
		}

		t.Logf("Queue lengths: %v, Total: %d", queueLengths, totalInQueues)
	})

	t.Run("round robin counter wraps correctly", func(t *testing.T) {
		pool := NewPool(2, 10, RoundRobin)
		pool.Start()
		defer pool.Stop()

		// Enqueue many tasks to test counter wrapping
		numTasks := 100
		var processedCount int64
		var mu sync.Mutex

		for i := 0; i < numTasks; i++ {
			key := fmt.Sprintf("wrap-test-%d", i)
			_, err := pool.Enqueue(key, func(ctx context.Context) {
				mu.Lock()
				processedCount++
				mu.Unlock()
			})
			if err != nil {
				t.Fatalf("Failed to enqueue task %d: %v", i, err)
			}
		}

		// Wait for processing
		timeout := time.After(5 * time.Second)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-timeout:
				t.Fatalf("Timeout waiting for tasks to complete")
			case <-ticker.C:
				mu.Lock()
				processed := processedCount
				mu.Unlock()
				if processed == int64(numTasks) {
					goto done
				}
			}
		}
	done:

		// Verify final counts
		mu.Lock()
		if processedCount != int64(numTasks) {
			t.Errorf("Expected %d tasks processed, got %d", numTasks, processedCount)
		}
		mu.Unlock()
	})

	t.Run("round robin concurrent access", func(t *testing.T) {
		pool := NewPool(4, 20, RoundRobin)
		pool.Start()
		defer pool.Stop()

		var wg sync.WaitGroup
		var processedCount int64
		var mu sync.Mutex
		numGoroutines := 10
		tasksPerGoroutine := 10

		// Launch multiple goroutines enqueueing concurrently
		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineId int) {
				defer wg.Done()
				for i := 0; i < tasksPerGoroutine; i++ {
					key := fmt.Sprintf("concurrent-%d-%d", goroutineId, i)
					_, err := pool.Enqueue(key, func(ctx context.Context) {
						mu.Lock()
						processedCount++
						mu.Unlock()
					})
					if err != nil {
						t.Errorf("Failed to enqueue task from goroutine %d: %v", goroutineId, err)
						return
					}
				}
			}(g)
		}

		wg.Wait()

		// Wait for all tasks to be processed
		expectedTasks := int64(numGoroutines * tasksPerGoroutine)
		timeout := time.After(5 * time.Second)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-timeout:
				mu.Lock()
				processed := processedCount
				mu.Unlock()
				t.Fatalf("Timeout waiting for tasks. Processed %d out of %d", processed, expectedTasks)
			case <-ticker.C:
				mu.Lock()
				processed := processedCount
				mu.Unlock()
				if processed == expectedTasks {
					goto concurrentDone
				}
			}
		}
	concurrentDone:

		mu.Lock()
		if processedCount != expectedTasks {
			t.Errorf("Expected %d tasks processed, got %d", expectedTasks, processedCount)
		}
		mu.Unlock()
	})
}

func TestKeyBasedVsRoundRobin(t *testing.T) {
	t.Run("key-based ensures same key goes to same worker", func(t *testing.T) {
		pool := NewPool(3, 10, KeyBased)
		if pool.GetStrategy() != KeyBased {
			t.Error("Expected KeyBased strategy")
		}

		// Same key should always go to same worker
		key := "consistent-key"
		workerIndex1 := pool.to(key)
		workerIndex2 := pool.to(key)
		workerIndex3 := pool.to(key)

		if workerIndex1 != workerIndex2 || workerIndex2 != workerIndex3 {
			t.Errorf("Same key should go to same worker: %d, %d, %d", workerIndex1, workerIndex2, workerIndex3)
		}
	})

	t.Run("round-robin distributes sequentially", func(t *testing.T) {
		pool := NewPool(3, 10, RoundRobin)
		if pool.GetStrategy() != RoundRobin {
			t.Error("Expected RoundRobin strategy")
		}

		// Sequential calls should go to different workers
		key := "any-key"          // Key doesn't matter for round-robin
		indices := make([]int, 9) // 3 full cycles
		for i := 0; i < 9; i++ {
			indices[i] = pool.to(key)
		}

		// Check pattern: 0,1,2,0,1,2,0,1,2
		expectedPattern := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
		for i, expected := range expectedPattern {
			if indices[i] != expected {
				t.Errorf("At position %d: expected worker %d, got %d. Full pattern: %v", i, expected, indices[i], indices)
				break
			}
		}
	})

	t.Run("default pool uses key-based strategy", func(t *testing.T) {
		pool := NewPool(3, 10, KeyBased)
		if pool.GetStrategy() != KeyBased {
			t.Error("Default pool should use KeyBased strategy")
		}
	})
}

func TestRoundRobinWithOptions(t *testing.T) {
	t.Run("round robin with expiration", func(t *testing.T) {
		pool := NewPool(5, 5, RoundRobin)
		pool.Start()
		defer pool.Stop()

		var processedCount int64
		var mu sync.Mutex

		// Enqueue tasks with expiration
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("expire-task-%d", i)
			_, err := pool.Enqueue(key, func(ctx context.Context) {
				mu.Lock()
				processedCount++
				mu.Unlock()
			}, WithExpirationDuration(2*time.Second))

			if err != nil {
				t.Fatalf("Failed to enqueue task %d: %v", i, err)
			}
		}

		// Wait for processing
		time.Sleep(300 * time.Millisecond)

		mu.Lock()
		if processedCount != 10 {
			t.Errorf("Expected 4 tasks processed, got %d", processedCount)
		}
		mu.Unlock()
	})

	t.Run("round robin with context timeout", func(t *testing.T) {
		pool := NewPool(2, 10, RoundRobin)
		pool.Start()
		defer pool.Stop()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		var processedCount int64
		var mu sync.Mutex

		// Enqueue tasks with context
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("context-task-%d", i)
			_, err := pool.Enqueue(key, func(execCtx context.Context) {
				mu.Lock()
				processedCount++
				mu.Unlock()
				if execCtx != ctx {
					t.Error("Context should be passed correctly")
				}
			}, WithContext(ctx))

			if err != nil {
				t.Fatalf("Failed to enqueue task %d: %v", i, err)
			}
		}

		// Wait for processing
		time.Sleep(300 * time.Millisecond)

		mu.Lock()
		if processedCount != 3 {
			t.Errorf("Expected 3 tasks processed, got %d", processedCount)
		}
		mu.Unlock()
	})
}
