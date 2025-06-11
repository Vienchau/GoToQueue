package gotoqueue

import (
	"log"
	"testing"
)

func TestHashDeterminismAndDistribution(t *testing.T) {
	mq := NewPool(4, 100, KeyBased)

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
	mq := NewPool(0, 0, KeyBased)
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
