package main

import "errors"

type Status string

const (
	StatusPending    Status = "pending"
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

var (
	ErrInvalidInput = errors.New("invalid expense input")
	ErrNotFound     = errors.New("task not found")
	ErrInvalidState = errors.New("invalid task state")
	ErrQueueFull    = errors.New("task queue is full")
	ErrPoolStopped  = errors.New("worker pool is stopped")
)

type Expense struct {
	Category    string
	Description string
	AmountCents int64
}

type Result struct {
	TotalCents      int64
	AverageCents    float64
	Highest         Expense
	ByCategoryCents map[string]int64
	Count           int
}

type Task struct {
	ID       int
	Expenses []Expense
	Status   Status
	Result   *Result
	Err      string // failure reason when Status == StatusFailed
}

func (t Task) Clone() Task {
	t.Expenses = append([]Expense(nil), t.Expenses...)
	if t.Result != nil {
		result := *t.Result
		result.ByCategoryCents = make(map[string]int64, len(t.Result.ByCategoryCents))
		for k, v := range t.Result.ByCategoryCents {
			result.ByCategoryCents[k] = v
		}
		t.Result = &result
	}
	return t
}
