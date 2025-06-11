package gotoqueue

import (
	"context"
	"log"
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
