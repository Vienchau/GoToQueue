package gotoqueue

import "errors"

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
