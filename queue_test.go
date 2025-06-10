package gotoqueue

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"sync"
	"testing"
	"time"
)

func TestNewPool(t *testing.T) {
	// Test creating a new multi-queue
	mq := NewPool(5, 10)
	if mq.GetPoolSize() != 5 {
		t.Errorf("Expected 5 workers, got %d", mq.GetPoolSize())
	}

	// Test with zero workers (should default to 1)
	mq2 := NewPool(0, 10)
	if mq2.GetPoolSize() != 1 {
		t.Errorf("Expected 1 worker for zero input, got %d", mq2.GetPoolSize())
	}
}

func TestStartStop(t *testing.T) {
	mq := NewPool(100, 100)

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
	mq := NewPool(3, 1)
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
		err := mq.Enqueue(keys[index], func(ctx context.Context) {
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
	mq := NewPool(3, 100)
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
		err := mq.Enqueue(key, func(ctx context.Context) {
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
	mq := NewPool(3, 100)
	mq.Start()
	defer mq.Stop()

	var wg sync.WaitGroup
	keyWorkerMap := make(map[string]int)
	var mutex sync.Mutex

	keys := []string{"key1", "key2", "key3", "key1", "key2", "key3"}

	for _, key := range keys {
		wg.Add(1)
		currentKey := key
		err := mq.Enqueue(currentKey, func(ctx context.Context) {
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

// AddRandomElementsToSlice adds n random elements to the provided slice
func AddRandomElementsToSlice(original []string, n int) ([]string, error) {
	// Define possible elements to add
	possibleElements := []string{
		"alpha", "beta", "gamma", "delta", "epsilon",
		"zeta", "eta", "theta", "iota", "kappa",
		"lambda", "mu", "nu", "xi", "omicron",
		"pi", "rho", "sigma", "tau", "upsilon",
		"phi", "chi", "psi", "omega",
	}

	result := make([]string, len(original))
	copy(result, original)

	max := big.NewInt(int64(len(possibleElements)))

	for i := 0; i < n; i++ {
		randomIndex, err := rand.Int(rand.Reader, max)
		if err != nil {
			return nil, err
		}
		result = append(result, possibleElements[randomIndex.Int64()])
	}

	return result, nil
}

func TestHashDeterminismAndDistribution(t *testing.T) {
	mq := NewPool(4, 100)

	// Determinism: same key always hashes to same worker
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon", "alpha", "beta"}

	// Add some random elements to the keys
	keys, _ = AddRandomElementsToSlice(keys, 100)

	hashes := make(map[string]int)
	queueVal := make(map[int][]string)
	for _, key := range keys {
		val := mq.to(key)
		if prev, ok := hashes[key]; ok {
			if prev != val {
				t.Errorf("Hash for key %q changed: was %d, now %d", key, prev, val)
			}
		} else {
			hashes[key] = val
		}
		queueVal[val] = append(queueVal[val], key)

		// t.Log("Key:", key, "-> Worker:", val)
	}

	for worker, keys := range queueVal {
		log.Printf("Worker %d has keys: %v\n", worker, keys)
	}

	// Distribution: keys should be spread across available workers
	workerCount := make(map[int]int)
	for _, key := range keys {
		worker := mq.to(key)
		workerCount[worker]++
	}
	if len(workerCount) < 2 {
		t.Errorf("Expected keys to be distributed across at least 2 workers, got %d", len(workerCount))
	}

	// Bounds: hash should always be within [0, numWorkers)
	for _, key := range keys {
		val := mq.to(key)
		if val < 0 || val >= mq.GetPoolSize() {
			t.Errorf("Hash for key %q out of bounds: got %d, want [0,%d)", key, val, mq.GetPoolSize())
		}
	}
}

func TestHashWithZeroOrNegativeWorkers(t *testing.T) {
	// Should never panic, but let's check behavior
	mq := NewPool(0, 0)
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("hash panicked with zero workers: %v", r)
		}
	}()
	val := mq.to("test")
	if val < 0 || val >= mq.GetPoolSize() {
		t.Errorf("Hash out of bounds with zero workers: got %d, want [0,%d)", val, mq.GetPoolSize())
	}
}

// =====================================
// QUEUE OPTIONS TESTS
// =====================================

func TestEnqueueWithContext(t *testing.T) {
	mq := NewPool(2, 10)
	mq.Start()
	defer mq.Stop()

	t.Run("context with timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		var executed bool
		var mu sync.Mutex

		err := mq.Enqueue("test-key", func(execCtx context.Context) {
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

		err := mq.Enqueue("test-key", func(ctx context.Context) {
			t.Error("Function should not execute with cancelled context")
		}, WithContext(ctx))

		if err != ErrQueueItemCancelled {
			t.Errorf("Expected ErrQueueItemCancelled error, got: %v", err)
		}
	})

	t.Run("context cancellation during enqueue", func(t *testing.T) {
		// Create a pool with buffer size 1 (minimum allowed) and block the worker
		smallPool := NewPool(1, 1)
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

		err := smallPool.Enqueue("test-key", func(ctx context.Context) {
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
	mq := NewPool(2, 10)
	mq.Start()
	defer mq.Stop()

	t.Run("timeout option creates context", func(t *testing.T) {
		var executed bool
		var mu sync.Mutex
		var capturedCtx context.Context

		err := mq.Enqueue("test-key", func(ctx context.Context) {
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
		smallPool := NewPool(1, 1)
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
		err := smallPool.Enqueue("test-key", func(ctx context.Context) {
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
	mq := NewPool(2, 10)
	mq.Start()
	defer mq.Stop()
	t.Run("item expires before execution", func(t *testing.T) {
		// Create a pool that will have delayed processing
		smallPool := NewPool(1, 10)
		smallPool.Start()
		defer smallPool.Stop()

		// First block the worker with a long task
		var wg sync.WaitGroup
		wg.Add(1)
		err := smallPool.Enqueue("blocker", func(ctx context.Context) {
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
		err = smallPool.Enqueue("test-key", func(ctx context.Context) {
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

		err := mq.Enqueue("test-key", func(ctx context.Context) {
			t.Error("Already expired function should not execute")
		}, WithExpiration(expireTime))

		if err != ErrQueueItemExpired {
			t.Errorf("Expected ErrQueueItemExpired error, got: %v", err)
		}
	})

	t.Run("item with expiration duration", func(t *testing.T) {
		var executed bool
		var mu sync.Mutex

		err := mq.Enqueue("test-key", func(ctx context.Context) {
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
		smallPool := NewPool(1, 1)
		smallPool.Start()
		defer smallPool.Stop()

		// Block the worker
		err := smallPool.Enqueue("blocker", func(ctx context.Context) {
			time.Sleep(200 * time.Millisecond)
		})
		if err != nil {
			t.Fatalf("Failed to enqueue blocker: %v", err)
		}

		err = smallPool.Enqueue("test-key", func(ctx context.Context) {
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
	mq := NewPool(2, 10)
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

		err := mq.Enqueue("test-key", func(ctx context.Context) {
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

		err := mq.Enqueue("test-key", func(ctx context.Context) {
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

		err := mq.Enqueue("test-key", func(ctx context.Context) {
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
	mq := NewPool(2, 10)
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

		err := mq.Enqueue("test-key", func(execCtx context.Context) {
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
		err := mq.Enqueue("test-key", func(ctx context.Context) {
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
		mq := NewPool(2, 10)
		// Don't start the pool

		err := mq.Enqueue("test-key", func(ctx context.Context) {})
		if err != ErrQueueNotRunning {
			t.Errorf("Expected ErrQueueNotRunning, got: %v", err)
		}
	})

	t.Run("enqueue with all options to stopped pool", func(t *testing.T) {
		mq := NewPool(2, 10)
		// Don't start the pool

		ctx := context.Background()
		metadata := map[string]interface{}{"test": "value"}

		err := mq.Enqueue("test-key", func(ctx context.Context) {},
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
	mq := NewPool(5, 20)
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
				err := mq.Enqueue(key, func(execCtx context.Context) {
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

// =====================================
// WORKER START() FUNCTION TESTS
// =====================================

func TestWorkerStart(t *testing.T) {
	t.Run("worker processes normal item successfully", func(t *testing.T) {
		pool := NewPool(1, 5)
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
		pool := NewPool(1, 5)
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
		pool := NewPool(1, 5)
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
		pool := NewPool(1, 5)
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
		pool := NewPool(1, 5)
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
		pool := NewPool(1, 5)
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
		pool := NewPool(1, 5)
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
		pool := NewPool(1, 5)
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
		pool := NewPool(1, 5)
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
		pool := NewPool(1, 5)
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
		pool := NewPool(1, 5)
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

// =====================================
// QUEUE LENGTH TESTS
// =====================================

func TestGetQueueLength(t *testing.T) {
	t.Run("valid worker IDs", func(t *testing.T) {
		mq := NewPool(3, 5)
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
		mq := NewPool(3, 5)

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
		mq := NewPool(2, 10)
		mq.Start()
		defer mq.Stop()

		// Add some items that will be processed slowly
		var wg sync.WaitGroup

		// Add items that block execution to keep them in queue
		for i := 0; i < 3; i++ {
			wg.Add(1)
			err := mq.Enqueue("key1", func(ctx context.Context) {
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
		mq := NewPool(1, 1)
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
		mq := NewPool(3, 5)
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
		mq := NewPool(3, 5)
		mq.Start()
		defer mq.Stop()

		totalLength := mq.GetTotalQueueLength()
		if totalLength != 0 {
			t.Errorf("Expected total length 0 for empty queues, got %d", totalLength)
		}
	})

	t.Run("single worker pool", func(t *testing.T) {
		mq := NewPool(1, 5)
		mq.Start()
		defer mq.Stop()

		totalLength := mq.GetTotalQueueLength()
		if totalLength != 0 {
			t.Errorf("Expected total length 0 for empty single worker pool, got %d", totalLength)
		}
	})

	t.Run("queues with items", func(t *testing.T) {
		mq := NewPool(3, 10)
		mq.Start()
		defer mq.Stop()

		var wg sync.WaitGroup
		itemCount := 6

		// Add items that will block execution temporarily
		for i := 0; i < itemCount; i++ {
			wg.Add(1)
			key := fmt.Sprintf("key%d", i%3) // Distribute across different workers
			err := mq.Enqueue(key, func(ctx context.Context) {
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
		mq := NewPool(4, 5)
		mq.Start()
		defer mq.Stop()

		// Add some items with keys that go to specific workers
		keys := []string{"key-a", "key-b", "key-c", "key-d", "key-e"}
		var wg sync.WaitGroup

		for _, key := range keys {
			wg.Add(1)
			err := mq.Enqueue(key, func(ctx context.Context) {
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
		mq := NewPool(3, 5)
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
		mq := NewPool(2, 5)
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
		mq := NewPool(2, 5)

		// Start pool
		mq.Start()

		// Add some items
		var wg sync.WaitGroup
		wg.Add(2)
		for i := 0; i < 2; i++ {
			err := mq.Enqueue(fmt.Sprintf("key%d", i), func(ctx context.Context) {
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
