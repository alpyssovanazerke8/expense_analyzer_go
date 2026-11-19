package main

import (
	"errors"
	"sync"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

// gateProcessor wraps the real processor and lets tests control timing
// with channels instead of sleeps.
type gateProcessor struct {
	ExpenseProcessor
	started chan struct{} // receives one value each time Process begins
	release chan struct{} // Process blocks until this is closed
}

func newGate() *gateProcessor {
	return &gateProcessor{started: make(chan struct{}, 100), release: make(chan struct{})}
}

func (g *gateProcessor) Process(e []Expense) (Result, error) {
	g.started <- struct{}{}
	<-g.release
	return g.ExpenseProcessor.Process(e)
}

func waitN(t *testing.T, ch <-chan struct{}, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-time.After(testTimeout):
			t.Fatalf("timed out waiting for event %d/%d", i+1, n)
		}
	}
}

func createTask(t *testing.T, s *Store, amount int64) Task {
	t.Helper()
	task, err := s.Create([]Expense{{Category: "Food", Description: "Item", AmountCents: amount}})
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func TestPoolStartupValidation(t *testing.T) {
	s := NewStore(ExpenseProcessor{})
	if _, err := NewPool(s, 1, 5); err == nil {
		t.Fatal("expected error for <2 workers")
	}
	if _, err := NewPool(s, 2, 0); err == nil {
		t.Fatal("expected error for zero capacity")
	}
	p, err := NewPool(s, 2, 5)
	if err != nil {
		t.Fatal(err)
	}
	p.Stop()
}

func TestSubmitAndProcessMany(t *testing.T) {
	s := NewStore(ExpenseProcessor{})
	p, err := NewPool(s, 3, 50)
	if err != nil {
		t.Fatal(err)
	}
	const n = 30
	ids := make([]int, n)
	for i := 0; i < n; i++ {
		task := createTask(t, s, int64(100*(i+1)))
		ids[i] = task.ID
		if err := p.Submit(task.ID); err != nil {
			t.Fatal(err)
		}
	}
	p.Stop()
	for i, id := range ids {
		got, err := s.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		want := int64(100 * (i + 1))
		if got.Status != StatusCompleted || got.Result == nil || got.Result.TotalCents != want {
			t.Fatalf("task %d wrong: %+v (want total %d)", id, got, want)
		}
	}
}

func TestSubmitDoesNotWaitForProcessing(t *testing.T) {
	g := newGate()
	s := NewStore(g)
	p, _ := NewPool(s, 2, 5)
	task := createTask(t, s, 100)
	if err := p.Submit(task.ID); err != nil { // returns although Process is blocked
		t.Fatal(err)
	}
	waitN(t, g.started, 1)
	got, _ := s.Get(task.ID)
	if got.Status != StatusProcessing {
		t.Fatalf("expected processing, got %s", got.Status)
	}
	close(g.release)
	p.Stop()
	got, _ = s.Get(task.ID)
	if got.Status != StatusCompleted {
		t.Fatalf("expected completed, got %s", got.Status)
	}
}

func TestTasksProcessConcurrently(t *testing.T) {
	g := newGate()
	s := NewStore(g)
	p, _ := NewPool(s, 2, 5)
	a, b := createTask(t, s, 100), createTask(t, s, 200)
	_ = p.Submit(a.ID)
	_ = p.Submit(b.ID)
	// Both Process calls must be running at the same time: each blocks until
	// release, so two "started" signals prove overlap.
	waitN(t, g.started, 2)
	close(g.release)
	p.Stop()
	for _, task := range []Task{a, b} {
		got, _ := s.Get(task.ID)
		if got.Status != StatusCompleted {
			t.Fatalf("task %d: %s", task.ID, got.Status)
		}
	}
}

func TestConcurrencyLimitedToWorkerCount(t *testing.T) {
	g := newGate()
	s := NewStore(g)
	p, _ := NewPool(s, 2, 10)
	for i := 0; i < 5; i++ {
		_ = p.Submit(createTask(t, s, 100).ID)
	}
	waitN(t, g.started, 2)
	select {
	case <-g.started:
		t.Fatal("more tasks running than workers")
	case <-time.After(50 * time.Millisecond):
	}
	close(g.release)
	p.Stop()
}

func TestInvalidAndDuplicateSubmission(t *testing.T) {
	g := newGate()
	s := NewStore(g)
	p, _ := NewPool(s, 2, 5)
	if err := p.Submit(999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	task := createTask(t, s, 100)
	if err := p.Submit(task.ID); err != nil {
		t.Fatal(err)
	}
	waitN(t, g.started, 1) // now Processing
	if err := p.Submit(task.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("resubmit while processing: %v", err)
	}
	close(g.release)
	p.Stop()
	got, _ := s.Get(task.ID)
	if got.Status != StatusCompleted {
		t.Fatalf("got %s", got.Status)
	}
	// Completed tasks cannot be submitted either (pool stopped -> use fresh pool).
	p2, _ := NewPool(s, 2, 5)
	defer p2.Stop()
	if err := p2.Submit(task.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("resubmit completed: %v", err)
	}
}

func TestQueueFull(t *testing.T) {
	g := newGate()
	s := NewStore(g)
	p, _ := NewPool(s, 2, 2)
	// Occupy both workers.
	_ = p.Submit(createTask(t, s, 100).ID)
	_ = p.Submit(createTask(t, s, 100).ID)
	waitN(t, g.started, 2)
	// Fill the queue (capacity 2).
	q1, q2 := createTask(t, s, 100), createTask(t, s, 100)
	if err := p.Submit(q1.ID); err != nil {
		t.Fatal(err)
	}
	if err := p.Submit(q2.ID); err != nil {
		t.Fatal(err)
	}
	over := createTask(t, s, 100)
	if err := p.Submit(over.ID); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected queue full, got %v", err)
	}
	got, _ := s.Get(over.ID)
	if got.Status != StatusPending {
		t.Fatalf("rejected task must stay pending, got %s", got.Status)
	}
	close(g.release)
	// Space is available again; rejected task can be resubmitted.
	deadline := time.After(testTimeout)
	for {
		err := p.Submit(over.ID)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrQueueFull) {
			t.Fatal(err)
		}
		select {
		case <-deadline:
			t.Fatal("queue never freed up")
		default:
		}
	}
	p.Stop()
	got, _ = s.Get(over.ID)
	if got.Status != StatusCompleted {
		t.Fatalf("got %s", got.Status)
	}
}

func TestFailedProcessingMarksFailed(t *testing.T) {
	s := NewStore(ExpenseProcessor{})
	// Bypass Create validation to craft a task whose processing fails.
	s.tasks[1] = &Task{ID: 1, Expenses: nil, Status: StatusPending}
	p, _ := NewPool(s, 2, 2)
	if err := p.Submit(1); err != nil {
		t.Fatal(err)
	}
	p.Stop()
	got, _ := s.Get(1)
	if got.Status != StatusFailed || got.Result != nil || got.Err == "" {
		t.Fatalf("expected failed task, got %+v", got)
	}
}

func TestConcurrentAccessSafe(t *testing.T) {
	s := NewStore(ExpenseProcessor{})
	p, _ := NewPool(s, 4, 100)
	var wg sync.WaitGroup
	const goroutines, perG = 8, 10
	var mu sync.Mutex
	totals := map[int]int64{}
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				amount := int64(g*1000 + i + 1)
				task, err := s.Create([]Expense{{Category: "C", Description: "D", AmountCents: amount}})
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				totals[task.ID] = amount
				mu.Unlock()
				if err := p.Submit(task.ID); err != nil {
					t.Error(err)
					return
				}
				_, _ = s.Get(task.ID) // concurrent read while workers write
			}
		}(g)
	}
	wg.Wait()
	p.Stop()
	if len(totals) != goroutines*perG {
		t.Fatalf("expected unique ids, got %d", len(totals))
	}
	for id, want := range totals {
		got, err := s.Get(id)
		if err != nil || got.Status != StatusCompleted || got.Result.TotalCents != want {
			t.Fatalf("task %d: %+v, %v (want %d)", id, got, err, want)
		}
	}
}

func TestStopDrainsAcceptedTasksAndTerminatesWorkers(t *testing.T) {
	g := newGate()
	s := NewStore(g)
	p, _ := NewPool(s, 2, 10)
	var ids []int
	for i := 0; i < 6; i++ {
		task := createTask(t, s, int64(100+i))
		ids = append(ids, task.ID)
		if err := p.Submit(task.ID); err != nil {
			t.Fatal(err)
		}
	}
	waitN(t, g.started, 2)
	done := make(chan struct{})
	go func() { p.Stop(); close(done) }()
	// Stop must wait while tasks are still running.
	select {
	case <-done:
		t.Fatal("Stop returned before accepted tasks finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(g.release)
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("Stop did not return: workers leaked or deadlocked")
	}
	for _, id := range ids {
		got, _ := s.Get(id)
		if got.Status != StatusCompleted {
			t.Fatalf("accepted task %d lost: %s", id, got.Status)
		}
	}
}

func TestSubmitAfterStopRejectedAndStopIdempotent(t *testing.T) {
	s := NewStore(ExpenseProcessor{})
	p, _ := NewPool(s, 2, 5)
	task := createTask(t, s, 100)
	p.Stop()
	p.Stop() // second call must not panic
	if err := p.Submit(task.ID); !errors.Is(err, ErrPoolStopped) {
		t.Fatalf("expected pool stopped, got %v", err)
	}
	got, _ := s.Get(task.ID)
	if got.Status != StatusPending {
		t.Fatalf("task must stay pending, got %s", got.Status)
	}
}

func TestSubmitRacingWithStop(t *testing.T) {
	s := NewStore(ExpenseProcessor{})
	p, _ := NewPool(s, 3, 200)
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := []int{}
	for i := 0; i < 50; i++ {
		task := createTask(t, s, 100)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := p.Submit(id)
			if err == nil {
				mu.Lock()
				accepted = append(accepted, id)
				mu.Unlock()
			} else if !errors.Is(err, ErrPoolStopped) && !errors.Is(err, ErrQueueFull) {
				t.Errorf("unexpected error: %v", err)
			}
		}(task.ID)
	}
	p.Stop() // concurrent with submits; must not panic
	wg.Wait()
	for _, id := range accepted {
		got, _ := s.Get(id)
		if got.Status != StatusCompleted {
			t.Fatalf("accepted task %d ended %s", id, got.Status)
		}
	}
}
