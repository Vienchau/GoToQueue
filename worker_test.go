package gotoqueue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
		_, err := pool.Enqueue("test-key", func(execCtx context.Context) {
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

// TestPanicRecovery tests various panic scenarios and recovery mechanisms
func TestPanicRecovery(t *testing.T) {
	t.Run("worker recovers from nil pointer dereference panic", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		pool.Start()
		defer pool.Stop()

		var panicCaught bool
		var mu sync.Mutex

		// Enqueue function that will panic with nil pointer dereference
		_, err := pool.Enqueue("panic-nil-ptr", func(ctx context.Context) {
			var nilPtr *string
			fmt.Println(*nilPtr) // This will panic
		})

		if err != nil {
			t.Fatalf("Failed to enqueue: %v", err)
		}

		// Wait for processing
		time.Sleep(200 * time.Millisecond)

		// Worker should still be running and able to process more items
		_, err = pool.Enqueue("after-panic", func(ctx context.Context) {
			mu.Lock()
			panicCaught = true
			mu.Unlock()
		})

		if err != nil {
			t.Fatalf("Failed to enqueue after panic: %v", err)
		}

		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		if !panicCaught {
			t.Error("Worker should be able to process items after panic recovery")
		}
		mu.Unlock()
	})

	t.Run("worker recovers from index out of bounds panic", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		pool.Start()
		defer pool.Stop()

		var recovered bool
		var mu sync.Mutex

		// Enqueue function that will panic with index out of bounds
		_, err := pool.Enqueue("panic-index", func(ctx context.Context) {
			slice := []int{1, 2, 3}
			fmt.Println(slice[10]) // This will panic
		})

		if err != nil {
			t.Fatalf("Failed to enqueue: %v", err)
		}

		// Wait for processing
		time.Sleep(200 * time.Millisecond)

		// Verify worker can still process items
		_, err = pool.Enqueue("verify-recovery", func(ctx context.Context) {
			mu.Lock()
			recovered = true
			mu.Unlock()
		})

		if err != nil {
			t.Fatalf("Failed to enqueue verification task: %v", err)
		}

		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		if !recovered {
			t.Error("Worker should recover and continue processing after index panic")
		}
		mu.Unlock()
	})

	t.Run("worker recovers from explicit panic call", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		pool.Start()
		defer pool.Stop()

		var afterPanicExecuted bool
		var mu sync.Mutex

		// Enqueue function that explicitly calls panic
		_, err := pool.Enqueue("explicit-panic", func(ctx context.Context) {
			panic("intentional panic for testing")
		})

		if err != nil {
			t.Fatalf("Failed to enqueue: %v", err)
		}

		// Wait for processing
		time.Sleep(200 * time.Millisecond)

		// Enqueue another task to verify worker is still functional
		_, err = pool.Enqueue("post-panic", func(ctx context.Context) {
			mu.Lock()
			afterPanicExecuted = true
			mu.Unlock()
		})

		if err != nil {
			t.Fatalf("Failed to enqueue post-panic task: %v", err)
		}

		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		if !afterPanicExecuted {
			t.Error("Worker should continue processing after explicit panic")
		}
		mu.Unlock()
	})

	t.Run("panic metadata is stored correctly", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		worker := pool.workers[0]

		// Set up a custom logger to capture panic logs
		var logOutput []string
		var logMu sync.Mutex
		worker.logger = &MockLogger{
			onErrorf: func(format string, v ...interface{}) {
				logMu.Lock()
				logOutput = append(logOutput, fmt.Sprintf(format, v...))
				logMu.Unlock()
			},
		}

		panicMessage := "test panic message"
		item := &QueueItem{
			key: "test-panic-metadata",
			fn: func(ctx context.Context) {
				panic(panicMessage)
			},
			ctx:         context.Background(),
			enqueueTime: time.Now(),
		}

		// Execute with panic recovery
		recovered, panicValue := worker.safeExecute(item)

		// Verify panic was recovered
		if !recovered {
			t.Error("Expected panic to be recovered")
		}

		if panicValue != panicMessage {
			t.Errorf("Expected panic value '%s', got '%v'", panicMessage, panicValue)
		}

		// Verify metadata was set correctly
		if item.metadata == nil {
			t.Fatal("Expected metadata to be set after panic")
		}

		if item.metadata["panic_recovered"] != true {
			t.Error("Expected panic_recovered to be true in metadata")
		}

		if item.metadata["panic_value"] != panicMessage {
			t.Errorf("Expected panic_value '%s' in metadata, got '%v'", panicMessage, item.metadata["panic_value"])
		}

		if item.metadata["worker_id"] != worker.id {
			t.Errorf("Expected worker_id %d in metadata, got %v", worker.id, item.metadata["worker_id"])
		}

		if _, exists := item.metadata["panic_time"]; !exists {
			t.Error("Expected panic_time to be set in metadata")
		}

		if _, exists := item.metadata["stack_trace"]; !exists {
			t.Error("Expected stack_trace to be set in metadata")
		}

		// Verify error was logged
		logMu.Lock()
		if len(logOutput) == 0 {
			t.Error("Expected panic to be logged")
		}
		logMu.Unlock()
	})

	t.Run("multiple panics in sequence are handled", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		pool.Start()
		defer pool.Stop()

		panicCount := 5
		var completedCount int
		var mu sync.Mutex

		// Enqueue multiple functions that will panic
		for i := 0; i < panicCount; i++ {
			i := i // Capture loop variable
			_, err := pool.Enqueue(fmt.Sprintf("panic-%d", i), func(ctx context.Context) {
				panic(fmt.Sprintf("panic number %d", i))
			})

			if err != nil {
				t.Fatalf("Failed to enqueue panic task %d: %v", i, err)
			}
		}

		// Wait for all panics to be processed
		time.Sleep(500 * time.Millisecond)

		// Enqueue normal tasks to verify worker is still functional
		for i := 0; i < 3; i++ {
			_, err := pool.Enqueue(fmt.Sprintf("normal-%d", i), func(ctx context.Context) {
				mu.Lock()
				completedCount++
				mu.Unlock()
			})

			if err != nil {
				t.Fatalf("Failed to enqueue normal task %d: %v", i, err)
			}
		}

		time.Sleep(300 * time.Millisecond)

		mu.Lock()
		if completedCount != 3 {
			t.Errorf("Expected 3 normal tasks to complete after panics, got %d", completedCount)
		}
		mu.Unlock()
	})

	t.Run("panic during context cancellation", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		pool.Start()
		defer pool.Stop()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		var executed bool
		var mu sync.Mutex

		// Enqueue function that panics and takes time
		_, err := pool.Enqueue("panic-with-context", func(execCtx context.Context) {
			time.Sleep(100 * time.Millisecond) // Sleep longer than context timeout
			panic("panic after context timeout")
		}, WithContext(ctx))

		if err != nil {
			t.Fatalf("Failed to enqueue: %v", err)
		}

		// Wait for context to timeout and processing to complete
		time.Sleep(200 * time.Millisecond)

		// Verify worker can still process items
		_, err = pool.Enqueue("after-context-panic", func(ctx context.Context) {
			mu.Lock()
			executed = true
			mu.Unlock()
		})

		if err != nil {
			t.Fatalf("Failed to enqueue after context panic: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		if !executed {
			t.Error("Worker should continue working after panic with context cancellation")
		}
		mu.Unlock()
	})

	t.Run("panic during shutdown draining", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		worker := pool.workers[0]

		var processedCount int
		var mu sync.Mutex

		// Create items - one that panics and one normal
		panicItem := QueueItem{
			key: "panic-during-drain",
			fn: func(ctx context.Context) {
				panic("panic during shutdown drain")
			},
			ctx:         context.Background(),
			enqueueTime: time.Now(),
		}

		normalItem := QueueItem{
			key: "normal-during-drain",
			fn: func(ctx context.Context) {
				mu.Lock()
				processedCount++
				mu.Unlock()
			},
			ctx:         context.Background(),
			enqueueTime: time.Now(),
		}

		// Start worker
		var wg sync.WaitGroup
		wg.Add(1)
		worker.wg = &wg
		go worker.start()

		// Add items to queue
		worker.queue <- panicItem
		worker.queue <- normalItem

		// Trigger shutdown
		close(worker.stopSignal)
		wg.Wait()

		// Verify normal item was processed despite panic
		mu.Lock()
		if processedCount != 1 {
			t.Errorf("Expected 1 normal item to be processed during drain, got %d", processedCount)
		}
		mu.Unlock()
	})
}

// MockLogger for testing panic logging
type MockLogger struct {
	onPrintf func(format string, v ...interface{})
	onErrorf func(format string, v ...interface{})
}

func (ml *MockLogger) Printf(format string, v ...interface{}) {
	if ml.onPrintf != nil {
		ml.onPrintf(format, v...)
	}
}

func (ml *MockLogger) Errorf(format string, v ...interface{}) {
	if ml.onErrorf != nil {
		ml.onErrorf(format, v...)
	}
}

// TestCustomPanicHandler tests custom panic handler functionality
func TestCustomPanicHandler(t *testing.T) {
	t.Run("custom panic handler is called", func(t *testing.T) {
		var handlerCalled bool
		var handlerItem *QueueItem
		var handlerPanicValue interface{}
		var handlerStackTrace []byte
		var mu sync.Mutex

		customHandler := func(item *QueueItem, panicValue interface{}, stackTrace []byte) {
			mu.Lock()
			handlerCalled = true
			handlerItem = item
			handlerPanicValue = panicValue
			handlerStackTrace = stackTrace
			mu.Unlock()
		}

		pool := NewPool(1, 5, KeyBased)
		worker := pool.workers[0]
		worker.panicHandler = customHandler
		worker.logger = &DefaultLogger{}

		panicMessage := "custom handler test panic"
		item := &QueueItem{
			key: "custom-handler-test",
			fn: func(ctx context.Context) {
				panic(panicMessage)
			},
			ctx:         context.Background(),
			enqueueTime: time.Now(),
		}

		// Execute with panic
		recovered, panicValue := worker.safeExecute(item)

		// Verify panic was recovered
		if !recovered {
			t.Error("Expected panic to be recovered")
		}

		if panicValue != panicMessage {
			t.Errorf("Expected panic value '%s', got '%v'", panicMessage, panicValue)
		}

		// Verify custom handler was called
		mu.Lock()
		if !handlerCalled {
			t.Error("Expected custom panic handler to be called")
		}

		if handlerItem.key != item.key {
			t.Errorf("Expected handler to receive item with key '%s', got '%s'", item.key, handlerItem.key)
		}

		if handlerPanicValue != panicMessage {
			t.Errorf("Expected handler to receive panic value '%s', got '%v'", panicMessage, handlerPanicValue)
		}

		if len(handlerStackTrace) == 0 {
			t.Error("Expected handler to receive non-empty stack trace")
		}
		mu.Unlock()
	})

	t.Run("default handler is used when no custom handler set", func(t *testing.T) {
		pool := NewPool(1, 5, KeyBased)
		worker := pool.workers[0]
		// Don't set custom handler - should use default

		var logOutput []string
		var logMu sync.Mutex
		worker.logger = &MockLogger{
			onErrorf: func(format string, v ...interface{}) {
				logMu.Lock()
				logOutput = append(logOutput, fmt.Sprintf(format, v...))
				logMu.Unlock()
			},
		}

		item := &QueueItem{
			key: "default-handler-test",
			fn: func(ctx context.Context) {
				panic("default handler test")
			},
			ctx:         context.Background(),
			enqueueTime: time.Now(),
		}

		// Execute with panic
		recovered, _ := worker.safeExecute(item)

		if !recovered {
			t.Error("Expected panic to be recovered")
		}

		// Verify error was logged (default handler behavior)
		logMu.Lock()
		if len(logOutput) == 0 {
			t.Error("Expected default handler to log error")
		}
		logMu.Unlock()
	})
}

// TestPanicRecoveryIntegration tests panic recovery in full integration scenarios
func TestPanicRecoveryIntegration(t *testing.T) {
	t.Run("multiple workers handle panics independently", func(t *testing.T) {
		workerCount := 3
		pool := NewPool(workerCount, 5, RoundRobin) // Use round-robin for even distribution
		pool.Start()
		defer pool.Stop()

		var completedTasks int64
		var panicTasks int64

		// Enqueue mix of normal and panic tasks
		for i := 0; i < 10; i++ {
			i := i
			if i%3 == 0 {
				// Every third task panics
				_, err := pool.Enqueue(fmt.Sprintf("panic-task-%d", i), func(ctx context.Context) {
					atomic.AddInt64(&panicTasks, 1)
					panic(fmt.Sprintf("panic from task %d", i))
				})
				if err != nil {
					t.Fatalf("Failed to enqueue panic task %d: %v", i, err)
				}
			} else {
				// Normal tasks
				_, err := pool.Enqueue(fmt.Sprintf("normal-task-%d", i), func(ctx context.Context) {
					atomic.AddInt64(&completedTasks, 1)
					time.Sleep(10 * time.Millisecond) // Simulate work
				})
				if err != nil {
					t.Fatalf("Failed to enqueue normal task %d: %v", i, err)
				}
			}
		}

		// Wait for all tasks to process
		time.Sleep(500 * time.Millisecond)

		// Verify expected counts
		expectedPanics := int64(4)    // Tasks 0, 3, 6, 9
		expectedCompleted := int64(6) // Tasks 1, 2, 4, 5, 7, 8

		if atomic.LoadInt64(&panicTasks) != expectedPanics {
			t.Errorf("Expected %d panic tasks, got %d", expectedPanics, atomic.LoadInt64(&panicTasks))
		}

		if atomic.LoadInt64(&completedTasks) != expectedCompleted {
			t.Errorf("Expected %d completed tasks, got %d", expectedCompleted, atomic.LoadInt64(&completedTasks))
		}
	})

	t.Run("high frequency panic recovery stress test", func(t *testing.T) {
		pool := NewPool(2, 10, KeyBased)
		pool.Start()
		defer pool.Stop()

		taskCount := 50
		var completed int64
		var panicked int64

		// Enqueue many tasks rapidly
		for i := 0; i < taskCount; i++ {
			i := i
			go func() {
				if i%2 == 0 {
					// Panic tasks
					_, err := pool.Enqueue(fmt.Sprintf("stress-panic-%d", i), func(ctx context.Context) {
						atomic.AddInt64(&panicked, 1)
						panic(fmt.Sprintf("stress panic %d", i))
					})
					if err != nil {
						t.Errorf("Failed to enqueue stress panic task %d: %v", i, err)
					}
				} else {
					// Normal tasks
					_, err := pool.Enqueue(fmt.Sprintf("stress-normal-%d", i), func(ctx context.Context) {
						atomic.AddInt64(&completed, 1)
					})
					if err != nil {
						t.Errorf("Failed to enqueue stress normal task %d: %v", i, err)
					}
				}
			}()
		}

		// Wait for processing
		time.Sleep(1 * time.Second)

		expectedPanics := int64(taskCount / 2)
		expectedCompleted := int64(taskCount / 2)

		if atomic.LoadInt64(&panicked) != expectedPanics {
			t.Errorf("Expected %d panic tasks, got %d", expectedPanics, atomic.LoadInt64(&panicked))
		}

		if atomic.LoadInt64(&completed) != expectedCompleted {
			t.Errorf("Expected %d completed tasks, got %d", expectedCompleted, atomic.LoadInt64(&completed))
		}
	})
}
