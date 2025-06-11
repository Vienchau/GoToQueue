# Go To Queue

> Just some code implement Key-based/Round-Robin Work Queue using Copilot

## Features

- **Multiple distribution strategies**:
  - **Key-based routing**: Items with same key go to same worker (sequential processing per key)
  - **Round-robin**: Items are distributed evenly across workers (load balancing)
- **Concurrent workers**: Multiple workers process tasks in parallel  
- **Context support**: Timeout and cancellation handling
- **Expiration**: Items can expire before processing
- **Metadata**: Attach custom data to queue items
- **Thread-safe**: Safe for concurrent use across multiple goroutines

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"
    
    "github.com/three-ball/go-to-queue"
)

func main() {
    // Create pool with 3 workers, buffer size 100, using key-based strategy
    pool := gotoqueue.NewPool(3, 100, gotoqueue.KeyBased)
    pool.Start()
    defer pool.Stop()

    // Basic enqueue
    pool.Enqueue("user:123", func(ctx context.Context) {
        fmt.Println("Processing user 123")
    })

    // With timeout
    pool.Enqueue("order:456", func(ctx context.Context) {
        fmt.Println("Processing order 456")
    }, gotoqueue.WithTimeout(5*time.Second))

    // With expiration
    pool.Enqueue("task:789", func(ctx context.Context) {
        fmt.Println("Processing task 789")
    }, gotoqueue.WithExpirationDuration(10*time.Minute))
    
    time.Sleep(100 * time.Millisecond) // Wait for processing
}
```

### Distribution Strategies

#### Key-Based Strategy
Items with the same key are always processed by the same worker, ensuring sequential processing per key:

```go
// Key-based: same keys go to same worker
pool := gotoqueue.NewPool(3, 100, gotoqueue.KeyBased)
pool.Enqueue("user:123", task1) // → Worker 0
pool.Enqueue("user:123", task2) // → Worker 0 (same worker)
pool.Enqueue("user:456", task3) // → Worker 1 (different worker)
```

#### Round-Robin Strategy
Items are distributed evenly across all workers regardless of key:

```go
// Round-robin: even distribution across workers
pool := gotoqueue.NewPool(3, 100, gotoqueue.RoundRobin)
pool.Enqueue("any-key", task1) // → Worker 0
pool.Enqueue("any-key", task2) // → Worker 1
pool.Enqueue("any-key", task3) // → Worker 2
pool.Enqueue("any-key", task4) // → Worker 0 (wraps around)
```

## Architecture Overview

### Worker Distribution

#### Key-Based Strategy
```
┌─────────────┐    Enqueue     ┌──────────────┐
│   Client    │ ──────────────▶│     Pool     │
└─────────────┘                └──────┬───────┘
                                      │
                               Hash(key) % workers
                                      │
                        ┌─────────────┼─────────────┐
                        ▼             ▼             ▼
                 ┌──────────┐  ┌──────────┐  ┌──────────┐
                 │ Worker 0 │  │ Worker 1 │  │ Worker 2 │
                 │ Queue    │  │ Queue    │  │ Queue    │
                 └─────┬────┘  └─────┬────┘  └─────┬────┘
                       │             │             │
                    FIFO            FIFO          FIFO
                   Processing     Processing    Processing
```

#### Round-Robin Strategy
```
┌─────────────┐    Enqueue     ┌──────────────┐
│   Client    │ ──────────────▶│     Pool     │
└─────────────┘                └──────┬───────┘
                                      │
                            counter++ % workers
                                      │
                        ┌─────────────┼─────────────┐
                        ▼             ▼             ▼
                 ┌──────────┐  ┌──────────┐  ┌──────────┐
                 │ Worker 0 │  │ Worker 1 │  │ Worker 2 │
                 │ Queue    │  │ Queue    │  │ Queue    │
                 └─────┬────┘  └─────┬────┘  └─────┬────┘
                       │             │             │
                    FIFO            FIFO          FIFO
                   Processing     Processing    Processing
```

### Enqueue Flow
```bash
[Client] ---> pool.Enqueue(key, fn, opts...)
                      │
                      ▼
              [Apply Options Pattern]
              options = applyEnqueueOptions(opts...)
                      │
                      ▼
              [Pre-flight Checks]
              ├── Pool running?
              ├── Item expired?
              ├── Context cancelled?
              └── Calculate worker index
                      │
                      ▼
              [Enqueueing Strategy]
              ┌─────────────────────────┐
              │ Has Context?            │
              └─────────┬───────────────┘
                       / \
                  YES /   \ NO
                     /     \
                    ▼       ▼
         [Context-Aware]   [Blocking]
         ┌─────────────┐   ┌─────────┐
         │ select {    │   │ channel │
         │   send      │   │ <- item │
         │   timeout   │   │ (wait)  │
         │ }           │   └─────────┘
         └─────────────┘       │
                │              │
                ▼              ▼
          [Return: nil       [Return: nil
           or ctx.Err()]     (always)]
```

## Usage Examples

### Basic Usage

#### Key-Based Strategy
```go
// Create and start pool with key-based routing
pool := gotoqueue.NewPool(4, 50, gotoqueue.KeyBased) // 4 workers, 50 buffer per worker
pool.Start()
defer pool.Stop()

// Tasks with same key go to same worker
pool.Enqueue("user:123", func(ctx context.Context) {
    fmt.Println("Process user 123 - task 1")
})
pool.Enqueue("user:123", func(ctx context.Context) {
    fmt.Println("Process user 123 - task 2") // Same worker as task 1
})
```

#### Round-Robin Strategy
```go
// Create and start pool with round-robin distribution
pool := gotoqueue.NewPool(4, 50, gotoqueue.RoundRobin) // 4 workers, 50 buffer per worker
pool.Start()
defer pool.Stop()

// Tasks are distributed evenly across workers
pool.Enqueue("task-1", func(ctx context.Context) {
    fmt.Println("Task 1") // → Worker 0
})
pool.Enqueue("task-2", func(ctx context.Context) {
    fmt.Println("Task 2") // → Worker 1
})
pool.Enqueue("task-3", func(ctx context.Context) {
    fmt.Println("Task 3") // → Worker 2
})
```

### Context and Timeout
```go
// With context cancellation
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

err := pool.Enqueue("task:456", func(ctx context.Context) {
    // Long running task
    select {
    case <-time.After(2 * time.Second):
        fmt.Println("Task completed")
    case <-ctx.Done():
        fmt.Println("Task cancelled:", ctx.Err())
    }
}, gotoqueue.WithContext(ctx))

// With timeout (creates context automatically)
err = pool.Enqueue("task:789", func(ctx context.Context) {
    time.Sleep(1 * time.Second)
    fmt.Println("Quick task done")
}, gotoqueue.WithTimeout(30*time.Second))
```

### Expiration
```go
// Expire at specific time
expireTime := time.Now().Add(1 * time.Hour)
pool.Enqueue("task:abc", myFunc, gotoqueue.WithExpiration(expireTime))

// Expire after duration
pool.Enqueue("task:def", myFunc, gotoqueue.WithExpirationDuration(5*time.Minute))
```

### Metadata
```go
// Add metadata to tasks
pool.Enqueue("order:123", func(ctx context.Context) {
    // Access metadata within the function if needed
    fmt.Println("Processing high priority order")
}, gotoqueue.WithMetadata(map[string]interface{}{
    "priority": "high",
    "user_id":  12345,
    "retry":    3,
}))
```

### Combined Options
```go
// Use multiple options together
pool.Enqueue("complex:task", func(ctx context.Context) {
    // Complex processing logic
    fmt.Println("Processing complex task with all options")
}, 
    gotoqueue.WithTimeout(10*time.Second),
    gotoqueue.WithExpirationDuration(5*time.Minute),
    gotoqueue.WithMetadata(map[string]interface{}{
        "priority": "critical",
        "retries":  5,
    }),
)
```

## Options Pattern

| Option | Description | Example |
|--------|-------------|---------|
| `WithContext(ctx)` | Provides context for cancellation | `WithContext(ctx)` |
| `WithTimeout(duration)` | Sets timeout (creates context) | `WithTimeout(30*time.Second)` |
| `WithExpiration(time)` | Item expires at specific time | `WithExpiration(time.Now().Add(1*time.Hour))` |
| `WithExpirationDuration(duration)` | Item expires after duration | `WithExpirationDuration(5*time.Minute)` |
| `WithMetadata(map)` | Attach custom metadata | `WithMetadata(map[string]interface{}{"key": "value"})` |

## Pool Management

```go
// Check pool status
if pool.IsRunning() {
    fmt.Println("Pool is active")
}

// Get pool information
poolSize := pool.GetPoolSize()
totalQueued := pool.GetTotalQueueLength()
workerQueue, err := pool.GetQueueLength(0) // Queue length for worker 0

fmt.Printf("Pool: %d workers, %d total items, worker 0 has %d items\n", 
    poolSize, totalQueued, workerQueue)
```