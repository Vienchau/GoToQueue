package gotoqueue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestEnqueueWithContext(t *testing.T) {
	mq := NewPool(2, 10, KeyBased)
	mq.Start()
	defer mq.Stop()

	t.Run("context with timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		var executed bool
		var mu sync.Mutex

		_, err := mq.Enqueue("test-key", func(execCtx context.Context) {
			mu.Lock()
			executed = true
			mu.Unlock()
			// Verify context is passed correctly
			if execCtx != ctx {
				t.Error("Expected execution context to match enqueue context")
			}
		}, WithContext(ctx))

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		// Wait for execution
		time.Sleep(200 * time.Millisecond)
		mu.Lock()
		if !executed {
			t.Error("Function should have been executed")
		}
		mu.Unlock()
	})

	t.Run("context cancellation before enqueue", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err := mq.Enqueue("test-key", func(ctx context.Context) {
			t.Error("Function should not execute with cancelled context")
		}, WithContext(ctx))

		if err != ErrQueueItemCancelled {
			t.Errorf("Expected ErrQueueItemCancelled error, got: %v", err)
		}
	})

	t.Run("context cancellation during enqueue", func(t *testing.T) {
		// Create a pool with buffer size 1 (minimum allowed) and block the worker
		smallPool := NewPool(1, 1, KeyBased)
		smallPool.Start()
		defer smallPool.Stop()

		// Use a channel to synchronize when the worker is busy
		workerBusy := make(chan struct{})
		workerStarted := make(chan struct{})

		// Fill the worker with a blocking task
		go func() {
			smallPool.Enqueue("blocker", func(ctx context.Context) {
				close(workerStarted) // Signal that worker has started
				<-workerBusy         // Wait for signal to continue
			})
		}()

		// Wait for worker to start processing
		<-workerStarted

		// Fill the buffer completely (1 slot)
		go func() {
			smallPool.Enqueue("buffer-filler", func(ctx context.Context) {
				// This will sit in the buffer
			})
		}()

		// Give time for buffer to fill
		time.Sleep(50 * time.Millisecond)

		// Now try to enqueue with a very short timeout - this should block and timeout
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err := smallPool.Enqueue("test-key", func(ctx context.Context) {
			t.Error("Function should not execute due to context timeout")
		}, WithContext(ctx))

		// Release the worker
		close(workerBusy)

		// The error should be context.DeadlineExceeded
		if err == nil {
			t.Error("Expected timeout error, got nil")
		} else if err != context.DeadlineExceeded {
			t.Errorf("Expected context.DeadlineExceeded error, got: %v", err)
		}
	})
}

func TestEnqueueWithTimeout(t *testing.T) {
	mq := NewPool(2, 10, KeyBased)
	mq.Start()
	defer mq.Stop()

	t.Run("timeout option creates context", func(t *testing.T) {
		var executed bool
		var mu sync.Mutex
		var capturedCtx context.Context

		_, err := mq.Enqueue("test-key", func(ctx context.Context) {
			mu.Lock()
			executed = true
			capturedCtx = ctx
			mu.Unlock()
		}, WithTimeout(1*time.Second))

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		if !executed {
			t.Error("Function should have been executed")
		}
		if capturedCtx == nil {
			t.Error("Context should not be nil")
		}
		mu.Unlock()

		// Verify context has deadline
		if _, ok := capturedCtx.Deadline(); !ok {
			t.Error("Context should have a deadline from WithTimeout")
		}
	})

	t.Run("short timeout causes cancellation", func(t *testing.T) {
		// Create a pool with buffer size 1 (minimum allowed) and block the worker
		smallPool := NewPool(1, 1, KeyBased)
		smallPool.Start()
		defer smallPool.Stop()

		// Use a channel to synchronize when the worker is busy
		workerBusy := make(chan struct{})
		workerStarted := make(chan struct{})

		// Fill the worker with a blocking task
		go func() {
			smallPool.Enqueue("blocker", func(ctx context.Context) {
				close(workerStarted) // Signal that worker has started
				<-workerBusy         // Wait for signal to continue
			})
		}()

		// Wait for worker to start processing
		<-workerStarted

		// Fill the buffer completely (1 slot)
		go func() {
			smallPool.Enqueue("buffer-filler", func(ctx context.Context) {
				// This will sit in the buffer
			})
		}()

		// Give time for buffer to fill
		time.Sleep(50 * time.Millisecond)

		// Now try to enqueue with timeout - this should block and timeout
		_, err := smallPool.Enqueue("test-key", func(ctx context.Context) {
			t.Error("Function should not execute due to timeout")
		}, WithTimeout(50*time.Millisecond))

		// Release the worker
		close(workerBusy)

		if err == nil {
			t.Error("Expected timeout error, got nil")
		} else if err != context.DeadlineExceeded {
			t.Errorf("Expected context.DeadlineExceeded error, got: %v", err)
		}
	})
}

func TestEnqueueWithExpiration(t *testing.T) {
	mq := NewPool(2, 10, KeyBased)
	mq.Start()
	defer mq.Stop()
	t.Run("item expires before execution", func(t *testing.T) {
		// Create a pool that will have delayed processing
		smallPool := NewPool(1, 10, KeyBased)
		smallPool.Start()
		defer smallPool.Stop()

		// First block the worker with a long task
		var wg sync.WaitGroup
		wg.Add(1)
		_, err := smallPool.Enqueue("blocker", func(ctx context.Context) {
			defer wg.Done()
			time.Sleep(200 * time.Millisecond) // Block the worker
		})
		if err != nil {
			t.Fatalf("Failed to enqueue blocker: %v", err)
		}

		// Now enqueue an item with very short expiration
		expireTime := time.Now().Add(10 * time.Millisecond) // Very short expiration

		var executed bool
		var mu sync.Mutex
		_, err = smallPool.Enqueue("test-key", func(ctx context.Context) {
			mu.Lock()
			executed = true
			mu.Unlock()
		}, WithExpiration(expireTime))

		if err != nil {
			t.Errorf("Unexpected error during enqueue: %v", err)
		}

		// Wait for the blocker to finish and give time for the expired item to be processed
		wg.Wait()
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		if executed {
			t.Error("Expired function should not have executed")
		}
		mu.Unlock()
	})

	t.Run("item already expired at enqueue time", func(t *testing.T) {
		expireTime := time.Now().Add(-1 * time.Second) // Already expired

		_, err := mq.Enqueue("test-key", func(ctx context.Context) {
			t.Error("Already expired function should not execute")
		}, WithExpiration(expireTime))

		if err != ErrQueueItemExpired {
			t.Errorf("Expected ErrQueueItemExpired error, got: %v", err)
		}
	})

	t.Run("item with expiration duration", func(t *testing.T) {
		var executed bool
		var mu sync.Mutex

		_, err := mq.Enqueue("test-key", func(ctx context.Context) {
			mu.Lock()
			executed = true
			mu.Unlock()
		}, WithExpirationDuration(1*time.Second))

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		if !executed {
			t.Error("Function should have executed before expiration")
		}
		mu.Unlock()
	})

	t.Run("item with short expiration duration", func(t *testing.T) {
		// Fill up queue to delay processing
		smallPool := NewPool(1, 1, KeyBased)
		smallPool.Start()
		defer smallPool.Stop()

		// Block the worker
		_, err := smallPool.Enqueue("blocker", func(ctx context.Context) {
			time.Sleep(200 * time.Millisecond)
		})
		if err != nil {
			t.Fatalf("Failed to enqueue blocker: %v", err)
		}

		_, err = smallPool.Enqueue("test-key", func(ctx context.Context) {
			t.Error("Function should not execute due to expiration")
		}, WithExpirationDuration(50*time.Millisecond))

		if err != nil {
			t.Errorf("Unexpected error during enqueue: %v", err)
		}

		// Wait for processing attempt
		time.Sleep(300 * time.Millisecond)
	})
}

func TestEnqueueWithMetadata(t *testing.T) {
	mq := NewPool(2, 10, KeyBased)
	mq.Start()
	defer mq.Stop()

	t.Run("basic metadata", func(t *testing.T) {
		metadata := map[string]interface{}{
			"user_id":    123,
			"request_id": "req-456",
			"priority":   "high",
		}

		var executed bool
		var mu sync.Mutex

		_, err := mq.Enqueue("test-key", func(ctx context.Context) {
			mu.Lock()
			executed = true
			mu.Unlock()
		}, WithMetadata(metadata))

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		if !executed {
			t.Error("Function should have been executed")
		}
		mu.Unlock()
	})

	t.Run("multiple metadata options", func(t *testing.T) {
		metadata1 := map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
		}
		metadata2 := map[string]interface{}{
			"key2": "updated_value2", // Should overwrite
			"key3": "value3",
		}

		var executed bool
		var mu sync.Mutex

		_, err := mq.Enqueue("test-key", func(ctx context.Context) {
			mu.Lock()
			executed = true
			mu.Unlock()
		}, WithMetadata(metadata1), WithMetadata(metadata2))

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		if !executed {
			t.Error("Function should have been executed")
		}
		mu.Unlock()
	})

	t.Run("nil metadata", func(t *testing.T) {
		var executed bool
		var mu sync.Mutex

		_, err := mq.Enqueue("test-key", func(ctx context.Context) {
			mu.Lock()
			executed = true
			mu.Unlock()
		}, WithMetadata(nil))

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		if !executed {
			t.Error("Function should have been executed")
		}
		mu.Unlock()
	})
}

func TestEnqueueCombinedOptions(t *testing.T) {
	mq := NewPool(2, 10, KeyBased)
	mq.Start()
	defer mq.Stop()

	t.Run("context, timeout, expiration, and metadata", func(t *testing.T) {
		ctx := context.Background()
		metadata := map[string]interface{}{
			"test": "combined_options",
		}

		var executed bool
		var mu sync.Mutex
		var capturedCtx context.Context

		_, err := mq.Enqueue("test-key", func(execCtx context.Context) {
			mu.Lock()
			executed = true
			capturedCtx = execCtx
			mu.Unlock()
		},
			WithContext(ctx),
			WithTimeout(2*time.Second),
			WithExpirationDuration(5*time.Second),
			WithMetadata(metadata),
		)

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		if !executed {
			t.Error("Function should have been executed")
		}
		if capturedCtx == nil {
			t.Error("Context should not be nil")
		}
		mu.Unlock()

		// Verify context has deadline from WithTimeout
		if _, ok := capturedCtx.Deadline(); !ok {
			t.Error("Context should have a deadline")
		}
	})

	t.Run("conflicting timeout and context", func(t *testing.T) {
		baseCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		var executed bool
		var mu sync.Mutex

		// WithTimeout should create a new context based on the existing one
		_, err := mq.Enqueue("test-key", func(ctx context.Context) {
			mu.Lock()
			executed = true
			mu.Unlock()
		}, WithContext(baseCtx), WithTimeout(500*time.Millisecond))

		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		if !executed {
			t.Error("Function should have been executed")
		}
		mu.Unlock()
	})
}

func TestQueueItemMethods(t *testing.T) {
	t.Run("QueueItem expiration methods", func(t *testing.T) {
		// Test non-expired item
		item := QueueItem{
			expireTime: time.Now().Add(1 * time.Hour),
		}
		if item.IsExpired() {
			t.Error("Item should not be expired")
		}

		// Test expired item
		item.expireTime = time.Now().Add(-1 * time.Hour)
		if !item.IsExpired() {
			t.Error("Item should be expired")
		}

		// Test item without expiration
		item.expireTime = time.Time{}
		if item.IsExpired() {
			t.Error("Item without expiration should never be expired")
		}
	})

	t.Run("QueueItem cancellation methods", func(t *testing.T) {
		// Test non-cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		item := QueueItem{ctx: ctx}
		if item.IsCancelled() {
			t.Error("Item should not be cancelled")
		}

		// Test cancelled context
		cancel()
		if !item.IsCancelled() {
			t.Error("Item should be cancelled")
		}

		// Test item without context
		item.ctx = nil
		if item.IsCancelled() {
			t.Error("Item without context should never be cancelled")
		}
	})

	t.Run("QueueItem metadata methods", func(t *testing.T) {
		item := QueueItem{}

		// Test getting from empty metadata
		value, exists := item.GetMetadata("key1")
		if exists || value != nil {
			t.Error("Should not find metadata in empty item")
		}

		// Test setting metadata
		item.SetMetadata("key1", "value1")
		value, exists = item.GetMetadata("key1")
		if !exists || value != "value1" {
			t.Error("Should find set metadata")
		}

		// Test setting multiple metadata
		item.SetMetadata("key2", 123)
		value, exists = item.GetMetadata("key2")
		if !exists || value != 123 {
			t.Error("Should find second metadata")
		}

		// Original key should still exist
		value, exists = item.GetMetadata("key1")
		if !exists || value != "value1" {
			t.Error("Original metadata should still exist")
		}
	})

	t.Run("QueueItem age calculation", func(t *testing.T) {
		enqueueTime := time.Now().Add(-1 * time.Second)
		item := QueueItem{enqueueTime: enqueueTime}

		age := item.GetAge()
		if age < 900*time.Millisecond || age > 1100*time.Millisecond {
			t.Errorf("Expected age around 1 second, got %v", age)
		}
	})
}

func TestEnqueueErrorCases(t *testing.T) {
	t.Run("enqueue to stopped pool", func(t *testing.T) {
		mq := NewPool(2, 10, KeyBased)
		// Don't start the pool

		_, err := mq.Enqueue("test-key", func(ctx context.Context) {})
		if err != ErrQueueNotRunning {
			t.Errorf("Expected ErrQueueNotRunning, got: %v", err)
		}
	})

	t.Run("enqueue with all options to stopped pool", func(t *testing.T) {
		mq := NewPool(2, 10, KeyBased)
		// Don't start the pool

		ctx := context.Background()
		metadata := map[string]interface{}{"test": "value"}

		_, err := mq.Enqueue("test-key", func(ctx context.Context) {},
			WithContext(ctx),
			WithTimeout(1*time.Second),
			WithExpirationDuration(2*time.Second),
			WithMetadata(metadata),
		)
		if err != ErrQueueNotRunning {
			t.Errorf("Expected ErrQueueNotRunning, got: %v", err)
		}
	})
}

func TestQueueOptionsEdgeCases(t *testing.T) {
	t.Run("WithTimeout on nil context", func(t *testing.T) {
		// This should create a background context
		options := applyEnqueueOptions(WithTimeout(1 * time.Second))

		if options.ctx == nil {
			t.Error("WithTimeout should create a context when none exists")
		}

		if _, ok := options.ctx.Deadline(); !ok {
			t.Error("WithTimeout should set a deadline")
		}
	})

	t.Run("WithTimeout on existing context", func(t *testing.T) {
		baseCtx := context.Background()
		options := applyEnqueueOptions(WithContext(baseCtx), WithTimeout(1*time.Second))

		if options.ctx == nil {
			t.Error("Should have a context")
		}

		if _, ok := options.ctx.Deadline(); !ok {
			t.Error("WithTimeout should set a deadline on existing context")
		}
	})

	t.Run("multiple WithMetadata calls", func(t *testing.T) {
		metadata1 := map[string]interface{}{"key1": "value1", "shared": "first"}
		metadata2 := map[string]interface{}{"key2": "value2", "shared": "second"}

		options := applyEnqueueOptions(WithMetadata(metadata1), WithMetadata(metadata2))

		if options.metadata["key1"] != "value1" {
			t.Error("First metadata should be preserved")
		}
		if options.metadata["key2"] != "value2" {
			t.Error("Second metadata should be added")
		}
		if options.metadata["shared"] != "second" {
			t.Error("Overlapping keys should be overwritten by later options")
		}
	})

	t.Run("empty metadata map", func(t *testing.T) {
		emptyMetadata := map[string]interface{}{}
		options := applyEnqueueOptions(WithMetadata(emptyMetadata))

		if options.metadata == nil {
			t.Error("Empty metadata map should initialize metadata field")
		}
		if len(options.metadata) != 0 {
			t.Error("Metadata should be empty")
		}
	})

	t.Run("default options", func(t *testing.T) {
		options := defaultEnqueueOptions()

		if options.ctx == nil {
			t.Error("Default options should have background context")
		}
		if options.metadata != nil {
			t.Error("Default options should have nil metadata")
		}
		if !options.expireTime.IsZero() {
			t.Error("Default options should have zero expiration time")
		}
	})
}

func TestConcurrentEnqueueWithOptions(t *testing.T) {
	mq := NewPool(5, 20, KeyBased)
	mq.Start()
	defer mq.Stop()
	t.Run("concurrent enqueue with different options", func(t *testing.T) {
		var enqueueWg sync.WaitGroup
		var executeWg sync.WaitGroup
		var mu sync.Mutex
		results := make(map[string]bool)
		var enqueueErrors []error

		// Pre-compute keys to avoid race conditions in string formatting
		keys := make([]string, 10)
		for i := 0; i < 10; i++ {
			keys[i] = fmt.Sprintf("key-%d", i)
		}

		// Launch multiple goroutines with different options
		for i := 0; i < 10; i++ {
			enqueueWg.Add(1)
			executeWg.Add(1) // Add to execute wait group before starting goroutine

			go func(id int, key string) {
				defer enqueueWg.Done()

				metadata := map[string]interface{}{
					"goroutine_id": id,
					"timestamp":    time.Now(),
				}

				// Use a much longer timeout or no timeout for this test
				_, err := mq.Enqueue(key, func(execCtx context.Context) {
					defer executeWg.Done()
					mu.Lock()
					results[key] = true
					mu.Unlock()
				},
					WithMetadata(metadata),
					WithExpirationDuration(30*time.Second), // Long expiration
				)

				if err != nil {
					executeWg.Done() // Done wasn't called in function, so call it here
					mu.Lock()
					enqueueErrors = append(enqueueErrors, fmt.Errorf("goroutine %d: %v", id, err))
					mu.Unlock()
				}
			}(i, keys[i])
		}

		// Wait for all enqueue operations to complete
		enqueueWg.Wait()

		// Check for enqueue errors first
		mu.Lock()
		numErrors := len(enqueueErrors)
		if numErrors > 0 {
			t.Logf("Got %d enqueue errors: %v", numErrors, enqueueErrors)
		}
		mu.Unlock()

		// Wait for all executions to complete
		done := make(chan struct{})
		go func() {
			executeWg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// All executions completed
		case <-time.After(3 * time.Second):
			mu.Lock()
			t.Errorf("Timeout waiting for executions. Got %d out of 10 executions. Errors: %d", len(results), numErrors)
			mu.Unlock()
			return
		}

		mu.Lock()
		if len(results) != 10 {
			t.Errorf("Expected 10 executions, got %d", len(results))
		}
		mu.Unlock()
	})
}
