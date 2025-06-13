[![Go Report Card](https://goreportcard.com/badge/github.com/three-ball/go-to-queue)](https://goreportcard.com/report/github.com/three-ball/go-to-queue)
[![GoDoc](https://godoc.org/github.com/three-ball/go-to-queue?status.svg)](https://godoc.org/github.com/three-ball/go-to-queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# Go To Queue

> Just a KeyBased/RoundRobin Work Queue coding with Copilot :)

![alt text](vibe_coding.png)

## Features

- **Multiple distribution strategies**:
  - **Key-based routing**: Items with same key go to same worker (sequential processing per key)
  - **Round-robin**: Items are distributed evenly across workers (load balancing)
- **Concurrent workers**: Multiple workers process tasks in parallel  
- **Context support**: Timeout and cancellation handling
- **Expiration**: Items can expire before processing
- **Metadata**: Attach custom data to queue items (automatically injected into context)
- **Thread-safe**: Safe for concurrent use across multiple goroutines
- **Panic recovery**: Workers automatically recover from panics and continue processing
- **Structured logging**: Configurable log levels (DEBUG, INFO, ERROR, SILENT)
- **Graceful shutdown**: Automatic OS signal handling with queue draining

## Installation

```bash
go get github.com/three-ball/go-to-queue
```

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

    // Basic enqueue with type-safe metadata
    pool.Enqueue("user:123", func(ctx context.Context) {
        if userID, ok := gotoqueue.GetMetadata(ctx, "user_id"); ok {
            fmt.Printf("Processing user: %v\n", userID)
        }
    }, gotoqueue.WithMetadata(map[string]interface{}{
        "user_id": "user:123",
    }))

    // With expiration duration
    pool.Enqueue("order:456", func(ctx context.Context) {
        fmt.Println("Processing order 456")
    }, gotoqueue.WithExpirationDuration(5*time.Second))
    
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
              [Blocking Enqueue]
              ┌─────────────────────────┐
              │ channel <- item         │
              │ (blocking send)         │
              └─────────────────────────┘
                      │
                      ▼
              [Return worker index]
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
```

### Expiration
```go
// Expire at specific time
expireTime := time.Now().Add(1 * time.Hour)
pool.Enqueue("task:abc", myFunc, gotoqueue.WithExpiration(expireTime))

// Expire after duration
pool.Enqueue("task:def", myFunc, gotoqueue.WithExpirationDuration(5*time.Minute))
```

### Metadata and Context Keys

The library provides type-safe metadata handling to avoid context key collisions:

```go
// Type-safe metadata access (recommended)
pool.Enqueue("order:123", func(ctx context.Context) {
    // Use GetMetadata for type-safe access
    if userID, ok := gotoqueue.GetMetadata(ctx, "user_id"); ok {
        fmt.Printf("User ID: %v\n", userID)
    }
    if priority, ok := gotoqueue.GetMetadata(ctx, "priority"); ok {
        fmt.Printf("Priority: %v\n", priority)
    }
}, gotoqueue.WithMetadata(map[string]interface{}{
    "priority": "high",
    "user_id":  12345,
    "retry":    3,
}))

// Legacy approach (not recommended due to context key collisions)
pool.Enqueue("order:456", func(ctx context.Context) {
    userID := ctx.Value("user_id") // Can cause collisions
    fmt.Printf("User ID: %v\n", userID)
}, gotoqueue.WithMetadata(map[string]interface{}{
    "user_id": "legacy-access",
}))
```

#### Type-Safe Context Keys

The library uses custom `ContextKey` types to prevent context key collisions:

```go
// Custom context key type prevents collisions
type ContextKey string

// MetadataKey creates a namespaced context key
func MetadataKey(key string) ContextKey {
    return ContextKey("metadata:" + key)
}

// GetMetadata provides type-safe metadata extraction
func GetMetadata(ctx context.Context, key string) (interface{}, bool) {
    value := ctx.Value(MetadataKey(key))
    return value, value != nil
}
```


## API Reference

### Core Functions

| Function | Description | Example |
|----------|-------------|---------|
| `NewPool(workers, buffer, strategy)` | Create new worker pool | `NewPool(4, 100, KeyBased)` |
| `pool.Start()` | Start the worker pool | `pool.Start()` |
| `pool.Stop()` | Stop the worker pool | `pool.Stop()` |
| `pool.Enqueue(key, fn, opts...)` | Add task to queue | `pool.Enqueue("user:123", taskFunc)` |

### Metadata Functions

| Function | Description | Example |
|----------|-------------|---------|
| `GetMetadata(ctx, key)` | Type-safe metadata extraction | `GetMetadata(ctx, "user_id")` |
| `MetadataKey(key)` | Create namespaced context key | `MetadataKey("custom_field")` |

### Options Pattern

| Option | Description | Example |
|--------|-------------|---------|
| `WithContext(ctx)` | Provides context for cancellation | `WithContext(ctx)` |
| `WithExpiration(time)` | Item expires at specific time | `WithExpiration(time.Now().Add(1*time.Hour))` |
| `WithExpirationDuration(duration)` | Item expires after duration | `WithExpirationDuration(5*time.Minute)` |
| `WithMetadata(map)` | Attach custom metadata (injected into context with type-safe keys) | `WithMetadata(map[string]interface{}{"key": "value"})` |

> **Note**: 
> - Metadata is automatically injected into context using type-safe keys to prevent collisions. Use `GetMetadata()` for extraction.

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

// Set logging level (DEBUG, INFO, ERROR, SILENT)
pool.SetLogLevel("DEBUG") // Enable debug logging
pool.SetLogLevel("SILENT") // Disable all logging (default)

// Alternative: use constants
pool.SetLogLevel(gotoqueue.LOG_DEBUG)
pool.SetLogLevel(gotoqueue.LOG_SILENT)

// Set custom panic handler
pool.SetPanicHandler(func(item *gotoqueue.QueueItem, panicValue interface{}, stackTrace []byte) {
    fmt.Printf("Custom panic handler: %v\n", panicValue)
})
```

## Error Handling & Recovery

The library includes robust error handling and panic recovery:

- **Automatic Panic Recovery**: Workers automatically recover from panics and continue processing
- **Panic Metadata**: Panic information is stored in item metadata for debugging
- **Custom Panic Handlers**: Configure custom panic handling logic
- **Structured Logging**: Debug information with configurable log levels

## Graceful Shutdown

The pool automatically handles OS signals (SIGINT, SIGTERM) for graceful shutdown:

- Signal handler drains all queues before stopping
- No manual `StopGracefully()` call needed
- Workers complete current tasks before shutdown

## Testing

Run the test suite:

```bash
go test ./...
```

Run tests with race detection:

```bash
go test -race ./...
```

## Examples

See the [examples](./examples) directory for complete working examples showcasing all features.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.