# SIS Expense Analyzer — Checkpoint 2

An in-memory Go application (concurrent worker pool added in Checkpoint 2) for analyzing expense batches. Each batch is a **task** with its own identifier, input, lifecycle state, and stored result.

## Requirements

- Go 1.27
- No third-party packages or external services

## Run

```bash
go run .
```

## Verify

```bash
gofmt -l .
go vet ./...
go test ./...
go test -race ./...
```

`gofmt -l .` should print nothing; vet and tests should succeed.

## Project files

- `task.go`: Task, Expense, Result, states, errors, and safe cloning
- `processor.go`: Processor interface, input validation, analysis
- `store.go`: Thread-safe in-memory task storage and state transitions
- `pool.go`: Worker pool, bounded queue, Submit and Stop
- `pool_test.go`: Concurrency tests (overlap, full queue, shutdown, races)
- `main.go`: Demo submitting three tasks to the pool
- `task_test.go`: Automated tests, including table-driven validation cases

## Input

Each task contains one or more expenses. Each expense has:
- `Category`: nonempty string
- `Description`: nonempty string
- `AmountCents`: positive integer amount in cents (e.g., 1599 means $15.99)

Using integer cents avoids floating-point rounding problems for totals.

## Processing result

The processor computes:
- Total amount
- Average amount
- Highest individual expense
- Total amount by category
- Number of expenses

## Lifecycle

```
Pending --Submit--> Queued --worker claims--> Processing --> Completed
   ^                   |                          |
   +-- queue full -----+                          +--> Failed
```

| From | To | When |
|---|---|---|
| Pending | Queued | `Pool.Submit` accepted the task into the queue |
| Queued | Pending | queue was full, submission rejected (rolled back) |
| Queued | Processing | a worker took the task from the queue |
| Pending | Processing | synchronous `Store.Process` (Checkpoint 1 API) |
| Processing | Completed | result stored |
| Processing | Failed | processing returned an error (`Task.Err` holds the reason) |

Submitting a Queued, Processing, Completed or Failed task returns `ErrInvalidState`; unknown IDs return `ErrNotFound`. Synchronous `Store.Process` still works as in Checkpoint 1 (on failure it returns the task to Pending).

## Concurrency design

```
Create -> Store (map + RWMutex) <----- Get (any goroutine)
             |  ^
   Submit    |  | claim / finish (short critical sections)
     |       v  |
     +--> [ bounded queue chan int, cap 10 ] --> Worker 1..3 --> Processor.Process (no lock held)
```

- **Workers:** `main` starts 3 (`NewPool(store, 3, 10)`); at least 2 are required. They loop `for id := range queue`, so they are reused for many tasks.
- **Queue:** a buffered `chan int` of task IDs; capacity is set in `NewPool` (10 in `main`). It never grows.
- **Full queue:** `Submit` is non-blocking and **rejects** with `ErrQueueFull`; the task is rolled back to Pending and can be resubmitted. Submit therefore can never hang on a stopped pool.
- **Submit** returns immediately after the task is queued; it does not wait for processing.
- **Synchronization:** `Store.mu` (`sync.RWMutex`) protects the map, the ID counter, *and* every field of stored tasks. Reads (`Get`) use RLock and return clones. State changes (`Pending->Queued`, `Queued->Processing`, ...) are check-and-set under one write lock, so two workers can never claim the same task. The lock is **not** held while a task is being calculated.
- **Shutdown:** `Pool.Stop()` takes the write lock of `Pool.mu`, sets `stopped`, closes the queue (only `Stop` closes it), then waits on a `sync.WaitGroup`. `Submit` holds the read lock while sending, so a send on a closed channel is impossible. Workers drain everything already queued before exiting, so accepted tasks are not lost. Calling `Stop` again is a no-op; `Submit` after `Stop` returns `ErrPoolStopped`.

## ## Design choices

- `Task`, `Expense`, and `Result` are structs.
- `Processor` is a meaningful interface so the store depends on behavior rather than a concrete calculator.
- `ExpenseProcessor` implements the interface with value receivers.
- `Store` uses pointer receivers because it modifies an in-memory `map[int]*Task` and an ID counter.
- The store clones inputs and returned tasks to prevent accidental external modification of stored data.
- Errors are returned, not panicked.
- No HTTP, database, context cancellation, or external APIs (reserved for future checkpoints).
- IDs are unique within the lifetime of one Store.

## Tests

`go test ./...` covers:
1. Successful creation
2. Invalid inputs (table-driven)
3. Successful retrieval
4. Missing task retrieval
5. Successful processing
6. Result correctness
7. State after processing
8. Invalid repeated processing
9. Multiple independent tasks and safe copies

Checkpoint 2 tests (`pool_test.go`) cover: pool startup, async submission, many tasks, proven overlapping processing (channel gates, no sleeps), worker-count limit, invalid/duplicate submission, full queue, failed tasks, concurrent access, shutdown draining and worker termination, rejection after shutdown, idempotent `Stop`, and `Submit` racing with `Stop`.

## Example

Expenses: Food/Lunch 1500 cents, Transport/Taxi 1000, Food/Dinner 2500, Shopping/Headphones 5000.

Result: total 10000 cents ($100.00), average 2500 cents ($25.00), highest Headphones 5000 cents ($50.00), categories Food 4000, Transport 1000, Shopping 5000.
