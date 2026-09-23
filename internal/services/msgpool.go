package services

import (
	"context"
	"hash/fnv"
	"runtime"
	"sync"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
	"velocity-engine-control-plane-backend-go/internal/metrics"
)

// workerPool decouples reading Kafka messages from processing them.
//
// Both consumers used to read a message and then run the whole pipeline for
// it — JSON unmarshal, schema normalization, LiveStore write, WebSocket
// fan-out — inline on the single goroutine that owns the Kafka client. That
// made throughput a function of one core: the pod could be given four, but
// the hot path could only ever use one of them, so at peak ingest the read
// loop simply fell behind and lag grew. The read loop now does nothing but
// hand raw bytes to this pool.
//
// Dispatch is KEY-AFFINE rather than round-robin. Each worker owns its own
// queue, and a message is routed to a worker by hashing its Kafka partition
// key, so every update for a given key is processed by the same worker in
// arrival order. Without that, two early-fire updates for the same window
// could be processed concurrently and land out of order. (LiveStore.Add's
// shouldReplace check is the second line of defence for the case where the
// producer's key isn't the grouping key — see its doc comment.)
type workerPool struct {
	queues  []chan []byte
	handler func([]byte)
	topic   string
	wg      sync.WaitGroup

	stopMetrics chan struct{}
	stopOnce    sync.Once
}

// newWorkerPool starts the pool. workers <= 0 means GOMAXPROCS.
func newWorkerPool(topic string, workers, totalQueueSize int, handler func([]byte)) *workerPool {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers < 1 {
		workers = 1
	}
	perQueue := totalQueueSize / workers
	if perQueue < 256 {
		perQueue = 256
	}

	p := &workerPool{
		queues:      make([]chan []byte, workers),
		handler:     handler,
		topic:       topic,
		stopMetrics: make(chan struct{}),
	}
	for i := range p.queues {
		p.queues[i] = make(chan []byte, perQueue)
		p.wg.Add(1)
		go p.worker(p.queues[i])
	}
	go p.reportDepth()
	return p
}

func (p *workerPool) worker(q chan []byte) {
	defer p.wg.Done()
	for msg := range q {
		p.handler(msg)
	}
}

// reportDepth publishes the combined queue depth once a second. Sampling on a
// ticker rather than setting the gauge per message keeps the hot path free of
// metric writes while still making saturation visible.
func (p *workerPool) reportDepth() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			depth := 0
			for _, q := range p.queues {
				depth += len(q)
			}
			metrics.KafkaConsumerQueueDepth.WithLabelValues(p.topic).Set(float64(depth))
		case <-p.stopMetrics:
			return
		}
	}
}

// submit hands one message to the worker that owns its key. If that worker's
// queue is full it blocks (until ctx is cancelled) rather than dropping the
// message: backpressure that shows up as consumer lag is recoverable and
// visible, silent data loss is neither. Returns false only if ctx ended
// first.
func (p *workerPool) submit(ctx context.Context, key []byte, value []byte) bool {
	q := p.queues[p.workerFor(key)]
	select {
	case q <- value:
		return true
	default:
	}

	metrics.KafkaConsumerQueueFullTotal.WithLabelValues(p.topic).Inc()
	select {
	case q <- value:
		return true
	case <-ctx.Done():
		return false
	}
}

// workerFor picks a worker index from the message key. A nil/empty key (a
// producer that isn't keying its records) falls back to worker 0, which
// preserves strict global ordering — correct, just not parallel, and the
// honest behaviour when there is no key to shard on.
func (p *workerPool) workerFor(key []byte) int {
	if len(p.queues) == 1 || len(key) == 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % uint32(len(p.queues)))
}

// stop closes the queues and waits for every in-flight message to finish
// processing. Called after the read loop has exited, so nothing can still be
// submitting.
func (p *workerPool) stop() {
	p.stopOnce.Do(func() {
		close(p.stopMetrics)
		for _, q := range p.queues {
			close(q)
		}
		p.wg.Wait()
	})
}

// kafkaTuning returns the throughput-related librdkafka settings shared by
// both consumers. The library's defaults (fetch.min.bytes=1,
// fetch.wait.max.ms=500) make the broker answer nearly every fetch
// immediately with whatever single record is available — fine at low rates,
// needless per-message overhead at ~1,000+/sec. Waiting briefly for a real
// batch costs a few ms of latency (well inside the WS flush interval that
// now paces delivery anyway) and cuts fetch round-trips substantially.
func kafkaTuning() map[string]interface{} {
	return map[string]interface{}{
		"fetch.min.bytes":            config.KafkaFetchMinBytes,
		"fetch.wait.max.ms":          config.KafkaFetchWaitMaxMs,
		"queued.max.messages.kbytes": config.KafkaQueuedMaxMessagesKb,
	}
}
