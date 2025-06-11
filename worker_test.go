package gotoqueue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWorkerStart(t *testing.T) {
	t.Run("worker processes normal item successfully", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		pool.Start()
		defer pool.Stop()

		var executed bool
		var mu sync.Mutex
		var executedCtx context.Context

		ctx := context.Background()
		err := pool.Enqueue("test-key", func(execCtx context.Context) {
			mu.Lock()
			executed = true
			executedCtx = execCtx
			mu.Unlock()
		}, WithContext(ctx))

		if err != nil {
			t.Fatalf("Failed to enqueue: %v", err)
		}

		// Wait for execution
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		if !executed {
			t.Error("Function should have been executed")
		}
		if executedCtx != ctx {
			t.Error("Context should have been passed correctly")
		}
		mu.Unlock()
	})

	t.Run("worker skips expired items", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		worker := pool.workers[0]

		// Create an expired item
		expiredItem := QueueItem{
			key:         "expired-key",
			fn:          func(ctx context.Context) { t.Error("Expired function should not execute") },
			ctx:         context.Background(),
			enqueueTime: time.Now().Add(-1 * time.Hour),
			expireTime:  time.Now().Add(-30 * time.Minute), // Expired 30 minutes ago
		}

		// Start worker
		var wg sync.WaitGroup
		wg.Add(1)
		worker.wg = &wg
		go worker.start()

		// Send expired item
		worker.queue <- expiredItem

		// Send stop signal to end test
		close(worker.stopSignal)
		wg.Wait()

		// If we get here without the function executing, the test passes
	})

	t.Run("worker skips cancelled items", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		worker := pool.workers[0]

		// Create a cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		cancelledItem := QueueItem{
			key:         "cancelled-key",
			fn:          func(ctx context.Context) { t.Error("Cancelled function should not execute") },
			ctx:         ctx,
			enqueueTime: time.Now(),
		}

		// Start worker
		var wg sync.WaitGroup
		wg.Add(1)
		worker.wg = &wg
		go worker.start()

		// Send cancelled item
		worker.queue <- cancelledItem

		// Send stop signal to end test
		close(worker.stopSignal)
		wg.Wait()

		// If we get here without the function executing, the test passes
	})

	t.Run("worker handles nil function gracefully", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		worker := pool.workers[0]

		nilFnItem := QueueItem{
			key:         "nil-fn-key",
			fn:          nil, // Nil function
			ctx:         context.Background(),
			enqueueTime: time.Now(),
		}

		// Start worker
		var wg sync.WaitGroup
		wg.Add(1)
		worker.wg = &wg
		go worker.start()

		// Send item with nil function
		worker.queue <- nilFnItem

		// Send stop signal to end test
		close(worker.stopSignal)
		wg.Wait()

		// Should complete without panic
	})

	t.Run("worker executes function without context", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		worker := pool.workers[0]

		var executed bool
		var mu sync.Mutex
		var executedCtx context.Context

		noCtxItem := QueueItem{
			key: "no-ctx-key",
			fn: func(ctx context.Context) {
				mu.Lock()
				executed = true
				executedCtx = ctx
				mu.Unlock()
			},
			ctx:         nil, // No context
			enqueueTime: time.Now(),
		}

		// Start worker
		var wg sync.WaitGroup
		wg.Add(1)
		worker.wg = &wg
		go worker.start()

		// Send item without context
		worker.queue <- noCtxItem

		// Wait for execution
		time.Sleep(50 * time.Millisecond)

		// Send stop signal to end test
		close(worker.stopSignal)
		wg.Wait()

		mu.Lock()
		if !executed {
			t.Error("Function should have been executed")
		}
		// Should have received background context
		if executedCtx == nil {
			t.Error("Should have received a context (background)")
		}
		mu.Unlock()
	})

	t.Run("worker handles context cancellation during execution", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		worker := pool.workers[0]

		ctx, cancel := context.WithCancel(context.Background())

		var executionStarted bool
		var mu sync.Mutex
		executionStartedChan := make(chan struct{})

		longRunningItem := QueueItem{
			key: "long-running-key",
			fn: func(execCtx context.Context) {
				mu.Lock()
				executionStarted = true
				mu.Unlock()
				close(executionStartedChan)

				// Simulate long-running work
				time.Sleep(200 * time.Millisecond)
			},
			ctx:         ctx,
			enqueueTime: time.Now(),
		}

		// Start worker
		var wg sync.WaitGroup
		wg.Add(1)
		worker.wg = &wg
		go worker.start()

		// Send long-running item
		worker.queue <- longRunningItem

		// Wait for execution to start
		<-executionStartedChan

		// Cancel context during execution
		cancel()

		// Wait a bit for cancellation to be detected
		time.Sleep(100 * time.Millisecond)

		// Send stop signal to end test
		close(worker.stopSignal)
		wg.Wait()

		mu.Lock()
		if !executionStarted {
			t.Error("Function execution should have started")
		}
		mu.Unlock()
	})

	t.Run("worker drains queue during shutdown", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		worker := pool.workers[0]

		var processedCount int
		var mu sync.Mutex

		// Start worker
		var wg sync.WaitGroup
		wg.Add(1)
		worker.wg = &wg
		go worker.start()

		// Add multiple items to queue
		for i := 0; i < 3; i++ {
			item := QueueItem{
				key: fmt.Sprintf("drain-key-%d", i),
				fn: func(ctx context.Context) {
					mu.Lock()
					processedCount++
					mu.Unlock()
				},
				ctx:         context.Background(),
				enqueueTime: time.Now(),
			}
			worker.queue <- item
		}

		// Send stop signal to trigger drainage
		close(worker.stopSignal)
		wg.Wait()

		mu.Lock()
		if processedCount != 3 {
			t.Errorf("Expected 3 items to be processed during shutdown, got %d", processedCount)
		}
		mu.Unlock()
	})

	t.Run("worker skips expired items during shutdown drainage", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		worker := pool.workers[0]

		var processedCount int
		var mu sync.Mutex

		// Start worker
		var wg sync.WaitGroup
		wg.Add(1)
		worker.wg = &wg
		go worker.start()

		// Add expired item
		expiredItem := QueueItem{
			key:         "expired-drain-key",
			fn:          func(ctx context.Context) { t.Error("Expired function should not execute during drain") },
			ctx:         context.Background(),
			enqueueTime: time.Now().Add(-1 * time.Hour),
			expireTime:  time.Now().Add(-30 * time.Minute),
		}
		worker.queue <- expiredItem

		// Add valid item
		validItem := QueueItem{
			key: "valid-drain-key",
			fn: func(ctx context.Context) {
				mu.Lock()
				processedCount++
				mu.Unlock()
			},
			ctx:         context.Background(),
			enqueueTime: time.Now(),
		}
		worker.queue <- validItem

		// Send stop signal to trigger drainage
		close(worker.stopSignal)
		wg.Wait()

		mu.Lock()
		if processedCount != 1 {
			t.Errorf("Expected 1 item to be processed during shutdown (expired should be skipped), got %d", processedCount)
		}
		mu.Unlock()
	})

	t.Run("worker skips cancelled items during shutdown drainage", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		worker := pool.workers[0]

		var processedCount int
		var mu sync.Mutex

		// Start worker
		var wg sync.WaitGroup
		wg.Add(1)
		worker.wg = &wg
		go worker.start()

		// Add cancelled item
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		cancelledItem := QueueItem{
			key:         "cancelled-drain-key",
			fn:          func(ctx context.Context) { t.Error("Cancelled function should not execute during drain") },
			ctx:         ctx,
			enqueueTime: time.Now(),
		}
		worker.queue <- cancelledItem

		// Add valid item
		validItem := QueueItem{
			key: "valid-drain-key",
			fn: func(ctx context.Context) {
				mu.Lock()
				processedCount++
				mu.Unlock()
			},
			ctx:         context.Background(),
			enqueueTime: time.Now(),
		}
		worker.queue <- validItem

		// Send stop signal to trigger drainage
		close(worker.stopSignal)
		wg.Wait()

		mu.Lock()
		if processedCount != 1 {
			t.Errorf("Expected 1 item to be processed during shutdown (cancelled should be skipped), got %d", processedCount)
		}
		mu.Unlock()
	})

	t.Run("worker handles nil function during shutdown drainage", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		worker := pool.workers[0]

		var processedCount int
		var mu sync.Mutex

		// Start worker
		var wg sync.WaitGroup
		wg.Add(1)
		worker.wg = &wg
		go worker.start()

		// Add item with nil function
		nilFnItem := QueueItem{
			key:         "nil-fn-drain-key",
			fn:          nil,
			ctx:         context.Background(),
			enqueueTime: time.Now(),
		}
		worker.queue <- nilFnItem

		// Add valid item
		validItem := QueueItem{
			key: "valid-drain-key",
			fn: func(ctx context.Context) {
				mu.Lock()
				processedCount++
				mu.Unlock()
			},
			ctx:         context.Background(),
			enqueueTime: time.Now(),
		}
		worker.queue <- validItem

		// Send stop signal to trigger drainage
		close(worker.stopSignal)
		wg.Wait()

		mu.Lock()
		if processedCount != 1 {
			t.Errorf("Expected 1 item to be processed during shutdown (nil function should be skipped), got %d", processedCount)
		}
		mu.Unlock()
	})

	t.Run("worker completes when queue is empty during shutdown", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		worker := pool.workers[0]

		// Start worker
		var wg sync.WaitGroup
		wg.Add(1)
		worker.wg = &wg
		go worker.start()

		// Send stop signal immediately (empty queue)
		close(worker.stopSignal)

		// Should complete quickly
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success - worker completed
		case <-time.After(1 * time.Second):
			t.Error("Worker should complete quickly when queue is empty during shutdown")
		}
	})
}
