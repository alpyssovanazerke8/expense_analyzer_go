package main

import (
	"fmt"
	"sync"
)

// Store is an in-memory task store safe for concurrent use.
// mu protects nextID, the tasks map AND every field of the Task values stored
// in it. Callers only ever receive clones, so no Task is shared outside the lock.
type Store struct {
	mu        sync.RWMutex
	nextID    int
	tasks     map[int]*Task
	processor Processor
}

func NewStore(processor Processor) *Store {
	return &Store{nextID: 1, tasks: make(map[int]*Task), processor: processor}
}

func (s *Store) Create(expenses []Expense) (Task, error) {
	if err := s.processor.Validate(expenses); err != nil {
		return Task{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task := &Task{
		ID:       s.nextID,
		Expenses: append([]Expense(nil), expenses...),
		Status:   StatusPending,
	}
	s.tasks[task.ID] = task
	s.nextID++
	return task.Clone(), nil
}

func (s *Store) Get(id int) (Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	task, exists := s.tasks[id]
	if !exists {
		return Task{}, fmt.Errorf("%w: id %d", ErrNotFound, id)
	}
	return task.Clone(), nil
}

// transition atomically moves a task from one state to another
// (check-then-set under a single write lock).
func (s *Store) transition(id int, from, to Status) (*Task, error) {
	task, exists := s.tasks[id]
	if !exists {
		return nil, fmt.Errorf("%w: id %d", ErrNotFound, id)
	}
	if task.Status != from {
		return nil, fmt.Errorf("%w: task %d is %s, expected %s", ErrInvalidState, id, task.Status, from)
	}
	task.Status = to
	return task, nil
}

// markQueued: Pending -> Queued.
func (s *Store) markQueued(id int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.transition(id, StatusPending, StatusQueued)
	return err
}

// unqueue: Queued -> Pending (used when the queue rejects the task).
func (s *Store) unqueue(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.transition(id, StatusQueued, StatusPending)
}

// claim moves a task from `from` to Processing and returns a copy of its
// expenses. Because the check and the write happen under one lock, only one
// goroutine can ever claim a given task.
func (s *Store) claim(id int, from Status) ([]Expense, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, err := s.transition(id, from, StatusProcessing)
	if err != nil {
		return nil, err
	}
	return append([]Expense(nil), task.Expenses...), nil
}

// finish stores the outcome of processing: Completed with a result, or Failed.
func (s *Store) finish(id int, result Result, procErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[id]
	if !ok {
		return
	}
	if procErr != nil {
		task.Status = StatusFailed
		task.Err = procErr.Error()
		return
	}
	task.Result = &result
	task.Status = StatusCompleted
}

// Process is the synchronous API from Checkpoint 1. The calculation runs
// outside the lock so other tasks can be read and processed meanwhile.
func (s *Store) Process(id int) (Task, error) {
	expenses, err := s.claim(id, StatusPending)
	if err != nil {
		return Task{}, err
	}
	result, err := s.processor.Process(expenses)
	if err != nil {
		s.mu.Lock()
		_, _ = s.transition(id, StatusProcessing, StatusPending)
		s.mu.Unlock()
		return Task{}, err
	}
	s.finish(id, result, nil)
	return s.Get(id)
}
