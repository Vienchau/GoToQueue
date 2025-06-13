package gotoqueue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

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
		}, WithContext(baseCtx))

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
			WithExpirationDuration(2*time.Second),
			WithMetadata(metadata),
		)
		if err != ErrQueueNotRunning {
			t.Errorf("Expected ErrQueueNotRunning, got: %v", err)
		}
	})
}

func TestQueueOptionsEdgeCases(t *testing.T) {

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
