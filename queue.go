// Package gotoqueue provides a high-performance, key-based worker queue implementation
// with FIFO ordering and concurrent processing capabilities.
//
// The library allows you to enqueue tasks with string keys, ensuring that tasks
// with the same key are processed sequentially by the same worker, while different
// keys can be processed concurrently by different workers.
//
// Key features:
//   - Key-based routing: Items with same key go to same worker
//   - Concurrent workers: Multiple workers process different keys in parallel
//   - Context support: Timeout and cancellation handling
//   - Expiration: Items can expire before processing
//   - Metadata: Attach custom data to queue items
//   - Thread-safe: Safe for concurrent use across multiple goroutines
//
// Example usage:
//
//	pool := gotoqueue.NewPool(3, 100) // 3 workers, 100 buffer per worker
//	pool.Start()
//	defer pool.Stop()
//
//	// Basic enqueue
//	pool.Enqueue("user:123", func(ctx context.Context) {
//		fmt.Println("Processing user 123")
//	})
//
//	// With timeout
//	pool.Enqueue("order:456", func(ctx context.Context) {
//		fmt.Println("Processing order 456")
//	}, gotoqueue.WithTimeout(5*time.Second))
package gotoqueue

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/spaolacci/murmur3"
)

var (
	// ErrQueueNotRunning is returned when trying to enqueue to a stopped queue
	ErrQueueNotRunning = errors.New("queue is not running")

	// ErrInvalidWorkerID is returned when an invalid worker ID is provided
	ErrInvalidWorkerID = errors.New("invalid worker ID")

	// ErrQueueItemExpired is returned when a queue item has expired
	ErrQueueItemExpired = errors.New("queue item has expired")

	// ErrQueueItemCancelled is returned when a queue item context is cancelled
	ErrQueueItemCancelled = errors.New("queue item context was cancelled")
)

// QueueItem represents an item in the queue with a key and a function to execute.
// The key is used to identify the item, and the function is the task to be executed.
// The same key will go to the same worker, ensuring that tasks with the same key are processed sequentially.
type QueueItem struct {
	key         string
	fn          func(context.Context)
	ctx         context.Context
	metadata    map[string]interface{}
	enqueueTime time.Time
	expireTime  time.Time
}

// IsExpired checks if the queue item has expired
func (qi *QueueItem) IsExpired() bool {
	if qi.expireTime.IsZero() {
		return false
	}
	return time.Now().After(qi.expireTime)
}

// IsCancelled checks if the queue item context is cancelled
func (qi *QueueItem) IsCancelled() bool {
	if qi.ctx == nil {
		return false
	}
	select {
	case <-qi.ctx.Done():
		return true
	default:
		return false
	}
}

// GetMetadata returns metadata value by key
func (qi *QueueItem) GetMetadata(key string) (interface{}, bool) {
	if qi.metadata == nil {
		return nil, false
	}
	value, exists := qi.metadata[key]
	return value, exists
}

// SetMetadata sets metadata value by key
func (qi *QueueItem) SetMetadata(key string, value interface{}) {
	if qi.metadata == nil {
		qi.metadata = make(map[string]interface{})
	}
	qi.metadata[key] = value
}

// GetAge returns how long the item has been in the queue
func (qi *QueueItem) GetAge() time.Duration {
	return time.Since(qi.enqueueTime)
}

// Worker is responsible for processing items from the queue.
// Each worker has a unique ID, a channel to receive items, and a stop signal.
// Workers will process items in the order they are received, and they will only process one item at a time.
// The stop signal is used to gracefully shut down the worker.
// The WaitGroup is used to wait for the worker to finish processing before shutting down.
type Worker struct {
	id         int
	queue      chan QueueItem
	stopSignal chan struct{}
	wg         *sync.WaitGroup
}

// start is the main worker loop - processes items in FIFO order
// and executes the function associated with each item.
// It listens for items on the queue and processes them until it receives a stop signal.
// If a stop signal is received, it drains the queue and processes any remaining items before shutting down.
func (w *Worker) start() {
	defer w.wg.Done()

	for {
		select {
		case item := <-w.queue:
			// Check if item is expired before processing
			if item.IsExpired() {
				log.Printf("Worker %d: Skipping expired item with key: %s (age: %v)",
					w.id, item.key, item.GetAge())
				continue
			}

			// Check if item context is cancelled before processing
			if item.IsCancelled() {
				log.Printf("Worker %d: Skipping cancelled item with key: %s", w.id, item.key)
				continue
			}

			// Execute the function with context awareness
			if item.fn != nil {
				// If item has context, monitor for cancellation during execution
				if item.ctx != nil {
					done := make(chan struct{})
					go func() {
						defer close(done)
						item.fn(item.ctx)
					}()

					select {
					case <-done:
						// Function completed successfully
						log.Printf("Worker %d: Completed item with key: %s (age: %v)",
							w.id, item.key, item.GetAge())
					case <-item.ctx.Done():
						// Context was cancelled during execution
						log.Printf("Worker %d: Item cancelled during execution with key: %s (reason: %v)",
							w.id, item.key, item.ctx.Err())
						// Note: We can't stop the running function, but we log the cancellation
					}
				} else {
					// No context, execute directly
					item.fn(context.Background())
					log.Printf("Worker %d: Completed item with key: %s (age: %v)",
						w.id, item.key, item.GetAge())
				}
			}

		case <-w.stopSignal:
			// Drain remaining items in the queue before shutting down
			log.Printf("Worker %d: Draining queue before shutdown", w.id)
			for {
				select {
				case item := <-w.queue:
					// Process remaining items if not expired/cancelled
					if !item.IsExpired() && !item.IsCancelled() && item.fn != nil {
						item.fn(item.ctx)
						log.Printf("Worker %d: Processed remaining item with key: %s during shutdown",
							w.id, item.key)
					}
				default:
					log.Printf("Worker %d: Shutdown complete", w.id)
					return
				}
			}
		}
	}
}

// Pool represents a pool of workers that can process items from the queue.
type Pool struct {
	workers   []*Worker
	size      int
	wg        sync.WaitGroup
	mutex     sync.Mutex
	isRunning bool
}

// to calculates the index of the worker based on the key.
func (p *Pool) to(k string) int {
	hash := murmur3.Sum32([]byte(k))
	return int(hash) % p.size
}

// NewPool creates a new worker pool with the specified size and buffer size.
// The pool size determines how many workers will be created, and the buffer size determines how many items can be buffered in each worker's queue.
// default of poolSize is 1 and bufferSize is 100 if they are not provided or are less than or equal to zero.
func NewPool(poolSize int, bufferSize int) *Pool {
	if poolSize <= 0 {
		poolSize = 1 // Ensure at least one worker
	}

	if bufferSize <= 0 {
		bufferSize = 100 // Ensure at least one item can be buffered
	}

	pool := &Pool{
		size:      poolSize,
		workers:   make([]*Worker, poolSize),
		isRunning: false,
	}

	for i := 0; i < poolSize; i++ {
		pool.workers[i] = &Worker{
			id:         i,
			queue:      make(chan QueueItem, bufferSize),
			stopSignal: make(chan struct{}),
			wg:         &pool.wg,
		}
	}

	return pool
}

// Start starts the worker pool.
// It initializes the workers and starts them in goroutines.
// If the pool is already running, it does nothing.
func (p *Pool) Start() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Check if the pool is already running
	if p.isRunning {
		return
	}

	p.isRunning = true

	// Start all workers
	for i := 0; i < p.size; i++ {
		p.wg.Add(1)
		go p.workers[i].start()
	}
}

// Stop stops the worker pool.
// It signals all workers to stop and waits for them to finish processing.
// If the pool is already stopped, it does nothing.
// It also closes all queues to prevent further enqueuing of items.
func (p *Pool) Stop() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Check if the pool is already stopped
	if !p.isRunning {
		return
	}

	p.isRunning = false

	// Signal all workers to stop
	for _, worker := range p.workers {
		close(worker.stopSignal)
	}

	// Wait for all workers to finish processing
	p.wg.Wait()

	// Close all queues
	for i := 0; i < p.size; i++ {
		close(p.workers[i].queue)
	}
}

// Enqueue adds a new item to the queue with optional configuration.
// This is the main method that uses the options pattern.
func (p *Pool) Enqueue(key string, fn func(context.Context), opts ...EnqueueOption) error {
	// Apply all options
	options := applyEnqueueOptions(opts...)

	p.mutex.Lock()
	isRunning := p.isRunning
	p.mutex.Unlock()

	// Check if the pool is running
	if !isRunning {
		return ErrQueueNotRunning
	}

	// Check if already expired
	if !options.expireTime.IsZero() && time.Now().After(options.expireTime) {
		return ErrQueueItemExpired
	}

	// Check if context is already cancelled
	if options.ctx != nil {
		select {
		case <-options.ctx.Done():
			return ErrQueueItemCancelled
		default:
		}
	}

	// Calculate the worker index based on the key
	workerIndex := p.to(key)

	// Copy metadata to avoid external modifications
	var itemMetadata map[string]interface{}
	if options.metadata != nil {
		itemMetadata = make(map[string]interface{})
		for k, v := range options.metadata {
			itemMetadata[k] = v
		}
	}

	item := QueueItem{
		key:         key,
		fn:          fn,
		ctx:         options.ctx,
		metadata:    itemMetadata,
		enqueueTime: time.Now(),
		expireTime:  options.expireTime,
	}

	log.Printf("Enqueuing item with key: %s to worker index: %d (has_context: %v, has_metadata: %v, expires: %v)",
		key, workerIndex, options.ctx != nil, options.metadata != nil, !options.expireTime.IsZero())

	// Try to enqueue with context awareness - wait until slot is available
	if options.ctx != nil {
		select {
		case p.workers[workerIndex].queue <- item:
			return nil
		case <-options.ctx.Done():
			return options.ctx.Err()
		}
	} else {
		// Blocking send - wait until queue has space
		p.workers[workerIndex].queue <- item
		return nil
	}
}

// GetQueueLength returns the length of the queue for a specific worker by its ID.
// It returns an error if the worker ID is invalid.
// The worker ID should be between 0 and size-1.
func (p *Pool) GetQueueLength(id int) (int, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if id < 0 || id >= p.size {
		return 0, ErrInvalidWorkerID
	}

	return len(p.workers[id].queue), nil
}

// GetTotalQueueLength returns the total number of items in all workers' queues.
func (p *Pool) GetTotalQueueLength() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	length := 0
	for _, worker := range p.workers {
		length += len(worker.queue)
	}
	return length
}

// GetPoolSize returns the number of workers in the pool.
// It returns the size of the pool, which is the number of workers.
func (p *Pool) GetPoolSize() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.size
}

// IsRunning checks if the worker pool is currently running.
// It returns true if the pool is running, false otherwise.
func (p *Pool) IsRunning() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.isRunning
}
