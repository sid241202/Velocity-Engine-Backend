package services

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestWorkerPool_KeyAffinityPreservesPerKeyOrder is the correctness guard on
// parallelising the consumer. Processing moved from one inline goroutine to a
// pool, so messages for the same key could in principle be handled
// concurrently and land out of order — an early-fire partial overwriting its
// window's final row. Dispatch is hashed by the Kafka record key so all
// messages for one key go to one worker; this asserts that ordering actually
// holds under concurrent submission.
func TestWorkerPool_KeyAffinityPreservesPerKeyOrder(t *testing.T) {
	var mu sync.Mutex
	seen := map[string][]int{}

	p := newWorkerPool("test-topic", 8, 4096, func(b []byte) {
		var key string
		var seq int
		fmt.Sscanf(string(b), "%s %d", &key, &seq)
		mu.Lock()
		seen[key] = append(seen[key], seq)
		mu.Unlock()
	})

	const keys = 16
	const perKey = 200
	ctx := context.Background()

	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := fmt.Sprintf("key%02d", k)
			for i := 0; i < perKey; i++ {
				p.submit(ctx, []byte(key), []byte(fmt.Sprintf("%s %d", key, i)))
			}
		}(k)
	}
	wg.Wait()
	p.stop()

	if len(seen) != keys {
		t.Fatalf("expected %d distinct keys processed, got %d", keys, len(seen))
	}
	for key, seqs := range seen {
		if len(seqs) != perKey {
			t.Fatalf("key %s: expected %d messages, got %d", key, perKey, len(seqs))
		}
		for i, got := range seqs {
			if got != i {
				t.Fatalf("key %s processed out of order at position %d: got %d", key, i, got)
			}
		}
	}
}

// TestWorkerPool_SubmitBlocksRatherThanDropsWhenFull verifies the backpressure
// choice: a saturated queue must never silently discard a Kafka message. The
// read loop blocks instead, which surfaces as consumer lag — visible and
// recoverable — rather than as data that simply never reaches the store.
func TestWorkerPool_SubmitBlocksRatherThanDropsWhenFull(t *testing.T) {
	release := make(chan struct{})
	var processed int
	var mu sync.Mutex

	// One worker, minimum queue depth (256), all blocked on `release`.
	p := newWorkerPool("test-topic", 1, 1, func(b []byte) {
		<-release
		mu.Lock()
		processed++
		mu.Unlock()
	})

	const n = 300 // > the 256 minimum queue depth
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			if !p.submit(context.Background(), []byte("k"), []byte("v")) {
				t.Errorf("submit reported failure with a live context")
				return
			}
		}
	}()

	select {
	case <-done:
		t.Fatal("submit drained a full queue without blocking — messages were dropped instead of applying backpressure")
	case <-time.After(200 * time.Millisecond):
		// Expected: submit is blocked waiting for queue space.
	}

	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("submit never unblocked after the workers drained")
	}
	p.stop()

	mu.Lock()
	defer mu.Unlock()
	if processed != n {
		t.Fatalf("expected all %d messages processed with no drops, got %d", n, processed)
	}
}

// TestWorkerPool_SubmitUnblocksOnContextCancel verifies a blocked submit
// releases on shutdown instead of wedging the read loop forever.
func TestWorkerPool_SubmitUnblocksOnContextCancel(t *testing.T) {
	block := make(chan struct{})
	p := newWorkerPool("test-topic", 1, 1, func(b []byte) { <-block })

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan bool, 1)
	go func() {
		for i := 0; i < 10000; i++ {
			if !p.submit(ctx, []byte("k"), []byte("v")) {
				result <- false
				return
			}
		}
		result <- true
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case ok := <-result:
		if ok {
			t.Fatal("expected submit to report cancellation, not completion")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("submit stayed blocked after its context was cancelled")
	}
	close(block)
	p.stop()
}
