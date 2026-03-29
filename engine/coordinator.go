package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/MarioCerulo/mapreduce/engine/types"
)

var (
	ErrWait = errors.New("no task available")
	ErrDone = errors.New("operation completed")
)

type taskRecord struct {
	id       int
	workerID string
	files    []string
}

type taskQueue struct {
	tasks []taskRecord
}

func (q *taskQueue) enqueue(t taskRecord) {
	q.tasks = append(q.tasks, t)
}

func (q *taskQueue) dequeue() (taskRecord, bool) {
	if len(q.tasks) == 0 {
		return taskRecord{}, false
	}
	t := q.tasks[0]
	q.tasks[0] = taskRecord{}
	q.tasks = q.tasks[1:]
	return t, true
}

type taskReply struct {
	task types.Task
	err  error
}

type taskRequest struct {
	workerID string
	reply    chan<- taskReply
}

type compReport struct {
	taskID int
	reply  chan<- error
}

type execPhase int

const (
	mapPhase execPhase = iota
	reducePhase
	donePhase
)

func (p execPhase) String() string {
	switch p {
	case mapPhase:
		return "map"
	case reducePhase:
		return "reduce"
	case donePhase:
		return "done"
	default:
		return "unknown"
	}
}

// Coordinator orchestrates the tasks between workers.
//
// The execution model is represented as a simple state machine.
type Coordinator struct {
	phase       execPhase
	numReducers int

	logger *slog.Logger

	pendingTasks    taskQueue
	inProgressTasks map[int]taskRecord
	completedTasks  []taskRecord

	requestsCh    chan taskRequest
	completionsCh chan compReport
}

// NewCoordinator initializes a job with one map task per input chunk.
func NewCoordinator(inputChunks []string, numReducers int, logger *slog.Logger) (*Coordinator, error) {
	if len(inputChunks) == 0 {
		return nil, errors.New("input chunks slice cannot be empty")
	}

	if numReducers < 1 {
		return nil, errors.New("the number of reducers must be greater than 0")
	}

	var tasks taskQueue
	for i, file := range inputChunks {
		tasks.enqueue(taskRecord{id: i, files: []string{file}})
	}

	logger = logger.With(slog.String("component", "coordinator"))

	return &Coordinator{
		phase:           mapPhase,
		numReducers:     numReducers,
		logger:          logger,
		pendingTasks:    tasks,
		inProgressTasks: make(map[int]taskRecord),
		requestsCh:      make(chan taskRequest),
		completionsCh:   make(chan compReport),
	}, nil
}

// Run handles the coordinator's event loop.
func (c *Coordinator) Run(ctx context.Context) {
	c.logger.Info("coordinator_started")
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("coordinator_stopped")
			return
		case req := <-c.requestsCh:
			c.logger.Debug("task_request_received",
				slog.String("worker_id", shortID(req.workerID)),
				slog.String("phase", c.phase.String()),
			)
			t, ok := c.pendingTasks.dequeue()
			if !ok {
				switch c.phase {
				case donePhase:
					req.reply <- taskReply{err: ErrDone}
					continue
				case mapPhase:
					if len(c.inProgressTasks) == 0 {
						panic("coordinator: empty in-progress tasks during map phase")
					}
					req.reply <- taskReply{err: ErrWait}
					c.logger.Debug("no_task_available_wait",
						slog.String("worker_id", shortID(req.workerID)),
						slog.String("phase", c.phase.String()),
					)
					continue
				case reducePhase:
					req.reply <- taskReply{err: ErrWait}
					continue
				default:
					c.logger.Error("invalid_phase_reached", slog.Any("phase_n", c.phase))
					panic("coordinator: invalid phase reached")
				}
			}

			var task types.Task
			switch c.phase {
			case mapPhase:
				task = types.Task{
					ID:        t.id,
					NReducers: c.numReducers,
					Kind:      types.MapTask,
					Files:     t.files,
				}
			case reducePhase:
				task = types.Task{
					ID:    t.id,
					Kind:  types.ReduceTask,
					Files: t.files,
				}
			}

			t.workerID = req.workerID
			c.inProgressTasks[t.id] = t

			req.reply <- taskReply{task: task}
			c.logger.Debug("task_assigned",
				slog.Int("task_id", t.id),
				slog.String("worker_id", shortID(req.workerID)),
				slog.String("phase", c.phase.String()),
			)

		case comp := <-c.completionsCh:
			t, ok := c.inProgressTasks[comp.taskID]
			if !ok {
				c.logger.Warn("completion_for_unknown_task",
					slog.Int("task_id", comp.taskID),
					slog.String("phase", c.phase.String()),
				)
				comp.reply <- fmt.Errorf("task %d not in progress", comp.taskID)
				continue
			}

			c.completedTasks = append(c.completedTasks, t)
			delete(c.inProgressTasks, t.id)

			c.logger.Debug("task_completed",
				slog.Int("task_id", t.id),
				slog.String("worker_id", shortID(t.workerID)),
				slog.String("phase", c.phase.String()),
			)

			if len(c.pendingTasks.tasks) == 0 && len(c.inProgressTasks) == 0 {
				if c.phase == reducePhase {
					c.phase = donePhase
					comp.reply <- nil
					c.logger.Info("phase_transition",
						slog.String("from", reducePhase.String()),
						slog.String("to", donePhase.String()),
						slog.Int("pending_tasks", len(c.pendingTasks.tasks)),
					)
					continue
				}

				for i := range c.numReducers {
					var files []string
					for tn := range len(c.completedTasks) {
						files = append(files, fmt.Sprintf("inter-%d-%d", tn, i))
					}
					c.pendingTasks.enqueue(taskRecord{id: i, files: files})
				}
				c.phase = reducePhase
				c.logger.Info("phase_transition",
					slog.String("from", mapPhase.String()),
					slog.String("to", reducePhase.String()),
				)
			}
			comp.reply <- nil
		}
	}
}

// RequestTask assigns a pending task to a worker.
//
// Returns [ErrWait] if all current tasks are in progress but not yet complete,
// or [ErrDone] if the job has finished and the worker should exit.
func (c *Coordinator) RequestTask(workerID string) (types.Task, error) {
	reply := make(chan taskReply, 1)
	req := taskRequest{
		workerID: workerID,
		reply:    reply,
	}
	c.requestsCh <- req
	res := <-reply
	return res.task, res.err
}

// ReportCompletion marks a task as completed and advances the job state.
//
// When all map tasks complete, reduce tasks are enqueued and the coordinator
// transitions to the reduce phase. When all reduce tasks complete, the job is
// marked as done.
func (c *Coordinator) ReportCompletion(taskID int) error {
	reply := make(chan error, 1)
	c.completionsCh <- compReport{taskID: taskID, reply: reply}
	return <-reply
}

func shortID(id string) string {
	if len(id) < 8 {
		return id
	}
	return id[:8]
}
