package gotoqueue

import (
	"context"
	"time"
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
	timeout     time.Duration // Timeout for the task execution
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
