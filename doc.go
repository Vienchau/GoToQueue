// Package gotoqueue provides a high-performance, key-based worker queue implementation
// with FIFO ordering and concurrent processing capabilities.
//
// The library allows you to enqueue tasks with string keys, ensuring that tasks
// with the same key are processed sequentially by the same worker, while different
// keys can be processed concurrently by different workers.
//
// # Distribution Strategies
//
// The package supports two distribution strategies:
//
//   - KeyBased: Items with the same key are always processed by the same worker,
//     ensuring sequential processing per key while allowing concurrent processing
//     of different keys.
//
//   - RoundRobin: Items are distributed evenly across all workers for optimal
//     load balancing, regardless of their keys.
//
// # Basic Usage
//
// Create a worker pool and start processing tasks:
//
//	pool := gotoqueue.NewPool(3, 100, gotoqueue.KeyBased)
//	pool.Start()
//	defer pool.Stop()
//
//	// Basic task
//	pool.Enqueue("user:123", func(ctx context.Context) {
//		fmt.Println("Processing user 123")
//	})
//
//	// Task with timeout
//	pool.Enqueue("order:456", func(ctx context.Context) {
//		fmt.Println("Processing order 456")
//	}, gotoqueue.WithTimeout(5*time.Second))
//
// # Features
//
//   - Multiple distribution strategies (Key-based and Round-robin)
//   - Concurrent workers with configurable pool size
//   - Context support for timeout and cancellation
//   - Item expiration functionality
//   - Metadata support for queue items
//   - Thread-safe operations
//   - FIFO ordering within each worker
//   - Pool statistics and monitoring
//   - Graceful shutdown support
//
// # Performance
//
// The library is optimized for high-throughput scenarios with:
//   - Efficient hash-based key distribution
//   - Lock-free operations where possible
//   - Minimal memory allocations
//   - Concurrent processing capabilities
//
// For more examples and advanced usage, see the example directory and documentation.
package gotoqueue
