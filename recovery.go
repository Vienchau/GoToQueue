package gotoqueue

import "log"

// PanicHandler defines how to handle panics
type PanicHandler func(item *QueueItem, panicValue interface{}, stackTrace []byte)

// Default panic handler
func DefaultPanicHandler(item *QueueItem, panicValue interface{}, stackTrace []byte) {
	log.Printf("PANIC in task execution - Key: %s, Panic: %v\nStack:\n%s",
		item.key, panicValue, string(stackTrace))
}
