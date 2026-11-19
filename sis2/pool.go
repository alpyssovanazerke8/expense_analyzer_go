package main

import (
	"errors"
	"fmt"
	"sync"
)

// Pool is a fixed-size worker pool fed by a bounded channel queue.
//
// Ownership:
//   - queue is created by NewPool; Submit only sends, workers only receive,
//     and Stop is the single place that closes it.
//   - mu + stopped guard the "send vs close" race: Submit holds the read lock
//     while sending, Stop takes the write lock before closing, so a send can
//     never hit a closed channel.
//
// Full queue policy: REJECT. Submit never blocks; it returns ErrQueueFull and
// the task goes back to Pending, so callers can never hang on a stopped pool.
type Pool struct {
	store *Store
	queue chan int
	wg    sync.WaitGroup

	mu      sync.RWMutex
	stopped bool
}

// NewPool starts `workers` goroutines (min 2) reading from a queue of
// `capacity` slots (min 1).
func NewPool(store *Store, workers, capacity int) (*Pool, error) {
	if workers < 2 {
		return nil, errors.New("worker pool needs at least 2 workers")
	}
	if capacity < 1 {
		return nil, errors.New("queue capacity must be at least 1")
	}
	p := &Pool{store: store, queue: make(chan int, capacity)}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p, nil
}

// Submit enqueues an existing Pending task and returns immediately.
func (p *Pool) Submit(id int) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.stopped {
		return ErrPoolStopped
	}
	// Pending -> Queued is atomic, so the same task cannot be queued twice.
	if err := p.store.markQueued(id); err != nil {
		return err
	}
	select {
	case p.queue <- id:
		return nil
	default:
		p.store.unqueue(id)
		return fmt.Errorf("%w: task %d not accepted", ErrQueueFull, id)
	}
}

func (p *Pool) worker() {
	defer p.wg.Done()
	// Ends when the queue is closed AND drained, so accepted tasks are never lost.
	for id := range p.queue {
		expenses, err := p.store.claim(id, StatusQueued)
		if err != nil {
			continue
		}
		result, procErr := p.store.processor.Process(expenses) // no lock held
		p.store.finish(id, result, procErr)
	}
}

// Stop rejects new submissions, lets workers finish everything already
// queued, and waits for them to exit. Safe to call more than once.
func (p *Pool) Stop() {
	p.mu.Lock()
	if !p.stopped {
		p.stopped = true
		close(p.queue)
	}
	p.mu.Unlock()
	p.wg.Wait()
}
