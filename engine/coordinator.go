package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/MarioCerulo/mapreduce/engine/types"
)

var (
	// ErrWait is returned when no task is available yet but the job is not complete.
	// The caller should back off and retry.
	ErrWait = errors.New("no task available")
	// ErrDone is returned when all tasks have completed and the worker should stop.
	ErrDone = errors.New("operation completed")
)

type taskRecord struct {
	id            int
	workerID      string
	files         []string
	lastHeartbeat time.Time
}

type taskQueue struct {
	tasks []taskRecord
}

func (q *taskQueue) enqueue(t taskRecord) {
	q.tasks = append(q.tasks, t)
}

func (q *taskQueue) enqueueAll(batch []taskRecord) {
	q.tasks = append(q.tasks, batch...)
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

type taskStore struct {
	pendingTasks    taskQueue
	inProgressTasks map[int]taskRecord
	completedTasks  []taskRecord

	workersMapping map[string]taskRecord
}

func (s *taskStore) batchEnqueue(batch []taskRecord) {
	s.pendingTasks.enqueueAll(batch)
}

func (s *taskStore) assignTask(workerID string) (taskRecord, bool) {
	t, ok := s.pendingTasks.dequeue()
	if !ok {
		return taskRecord{}, false
	}

	t.workerID = workerID
	t.lastHeartbeat = time.Now()
	s.workersMapping[workerID] = t

	s.inProgressTasks[t.id] = t

	return t, true
}

func (s *taskStore) completeTask(taskID int) (string, error) {
	t, ok := s.inProgressTasks[taskID]
	if !ok {
		return "", fmt.Errorf("task %d not in progress", taskID)
	}

	s.completedTasks = append(s.completedTasks, t)
	delete(s.inProgressTasks, t.id)
	delete(s.workersMapping, t.workerID)

	return t.workerID, nil
}

func (s *taskStore) heartbeat(workerID string) error {
	if task, ok := s.workersMapping[workerID]; ok {
		task.lastHeartbeat = time.Now()
		s.inProgressTasks[task.id] = task
		s.workersMapping[workerID] = task

		return nil
	}

	return fmt.Errorf("failed to acknowledge heartbeat for worker %s", workerID)
}

func (s *taskStore) requeueTask(taskID int) bool {
	t, ok := s.inProgressTasks[taskID]
	if !ok {
		return false
	}

	delete(s.inProgressTasks, t.id)

	if current, ok := s.workersMapping[t.workerID]; ok && current.id == t.id {
		delete(s.workersMapping, t.workerID)
	}

	t.workerID = ""
	t.lastHeartbeat = time.Time{}

	s.pendingTasks.enqueue(t)
	return true
}

func (s *taskStore) numPendingTasks() int {
	return len(s.pendingTasks.tasks)
}

func (s *taskStore) numInProgressTasks() int {
	return len(s.inProgressTasks)
}

func (s *taskStore) numCompletedTasks() int {
	return len(s.completedTasks)
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

// HeartbeatConfig controls how the coordinator detects unresponsive workers.
type HeartbeatConfig struct {
	// TTL is the expected interval between heartbeats from a worker.
	TTL time.Duration
	// MaxMissed is the number of consecutive missed heartbeats before the
	// worker's task is considered lost and requeued.
	MaxMissed int
}

// Coordinator orchestrates the tasks between workers.
//
// The execution model is represented as a simple state machine.
type Coordinator struct {
	phase       execPhase
	numReducers int
	hbConfig    HeartbeatConfig

	logger    *slog.Logger
	taskStore taskStore

	requestsCh    chan taskRequest
	completionsCh chan compReport
	heartbeatCh   chan string
}

// NewCoordinator initializes a job with one map task per input chunk.
func NewCoordinator(inputChunks []string, numReducers int, hbConfig HeartbeatConfig, logger *slog.Logger) (*Coordinator, error) {
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
		phase:       mapPhase,
		numReducers: numReducers,
		hbConfig:    hbConfig,
		logger:      logger,
		taskStore: taskStore{
			pendingTasks:    tasks,
			inProgressTasks: make(map[int]taskRecord),
			workersMapping:  make(map[string]taskRecord),
		},
		requestsCh:    make(chan taskRequest),
		completionsCh: make(chan compReport),
		heartbeatCh:   make(chan string),
	}, nil
}

// Run handles the coordinator's event loop.
func (c *Coordinator) Run(ctx context.Context) {
	c.logger.Info("coordinator_started")

	ticker := time.NewTicker(c.hbConfig.TTL)
	defer ticker.Stop()

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
			t, ok := c.taskStore.assignTask(req.workerID)
			if !ok {
				switch c.phase {
				case donePhase:
					req.reply <- taskReply{err: ErrDone}
					continue
				case mapPhase:
					if c.taskStore.numInProgressTasks() == 0 {
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

			req.reply <- taskReply{task: task}
			c.logger.Debug("task_assigned",
				slog.Int("task_id", t.id),
				slog.String("worker_id", shortID(req.workerID)),
				slog.String("phase", c.phase.String()),
			)

		case comp := <-c.completionsCh:
			workerID, err := c.taskStore.completeTask(comp.taskID)
			if err != nil {
				c.logger.Warn("completion_for_unknown_task",
					slog.Int("task_id", comp.taskID),
					slog.String("phase", c.phase.String()),
				)
				comp.reply <- err
				continue
			}

			c.logger.Debug("task_completed",
				slog.Int("task_id", comp.taskID),
				slog.String("worker_id", shortID(workerID)),
				slog.String("phase", c.phase.String()),
			)

			if c.taskStore.numPendingTasks() == 0 && c.taskStore.numInProgressTasks() == 0 {
				if c.phase == reducePhase {
					c.phase = donePhase
					comp.reply <- nil
					c.logger.Info("phase_transition",
						slog.String("from", reducePhase.String()),
						slog.String("to", donePhase.String()),
						slog.Int("pending_tasks", c.taskStore.numPendingTasks()),
					)
					continue
				}

				// Populate pending tasks with reduce tasks
				reduceTasks := make([]taskRecord, 0, c.numReducers)
				for i := range c.numReducers {
					var files []string
					for tn := range c.taskStore.numCompletedTasks() {
						files = append(files, fmt.Sprintf("inter-%d-%d", tn, i))
					}
					reduceTasks = append(reduceTasks, taskRecord{id: i, files: files})
				}
				c.taskStore.batchEnqueue(reduceTasks)

				c.phase = reducePhase
				c.logger.Info("phase_transition",
					slog.String("from", mapPhase.String()),
					slog.String("to", reducePhase.String()),
				)
			}
			comp.reply <- nil

		case workerID := <-c.heartbeatCh:
			if err := c.taskStore.heartbeat(workerID); err == nil {
				c.logger.Debug("heartbeat", slog.String("worker_id", workerID))
			}

		case <-ticker.C:
			now := time.Now()
			timeout := time.Duration(c.hbConfig.MaxMissed) * c.hbConfig.TTL
			toRequeue := make([]taskRecord, 0, c.taskStore.numInProgressTasks())

			for _, t := range c.taskStore.inProgressTasks {
				if now.After(t.lastHeartbeat.Add(timeout)) {
					toRequeue = append(toRequeue, t)
				}
			}

			for _, t := range toRequeue {
				c.logger.Warn("task_timeout",
					slog.Int("task_id", t.id),
					slog.String("worker_id", t.workerID),
				)

				if !c.taskStore.requeueTask(t.id) {
					c.logger.Warn("requeue_failed_not_in_progress",
						slog.Int("task_id", t.id),
					)
				}
			}
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

// Heartbeat records a liveness signal from the given worker, resetting its timeout window.
func (c *Coordinator) Heartbeat(workerID string) {
	c.heartbeatCh <- workerID
}

func shortID(id string) string {
	if len(id) < 8 {
		return id
	}
	return id[:8]
}
