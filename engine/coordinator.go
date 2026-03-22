package engine

import (
	"errors"
	"fmt"

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

type execPhase int

const (
	mapPhase execPhase = iota
	reducePhase
	donePhase
)

// Coordinator orchestrates the tasks between workers.
//
// The execution model is represented as a simple state machine.
type Coordinator struct {
	phase       execPhase
	numReducers int

	pendingTasks    taskQueue
	inProgressTasks map[int]taskRecord
	completedTasks  []taskRecord
}

// NewCoordinator initializes a job with one map task per input chunk.
func NewCoordinator(inputChunks []string, numReducers int) (*Coordinator, error) {
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
	return &Coordinator{
		phase:           mapPhase,
		numReducers:     numReducers,
		pendingTasks:    tasks,
		inProgressTasks: make(map[int]taskRecord),
	}, nil
}

// RequestTask assigns a pending task to a worker.
//
// Returns [ErrWait] if all current tasks are in progress but not yet complete,
// or [ErrDone] if the job has finished and the worker should exit.
func (c *Coordinator) RequestTask(workerID string) (types.Task, error) {
	t, ok := c.pendingTasks.dequeue()
	if !ok {
		switch c.phase {
		case donePhase:
			return types.Task{}, ErrDone
		case mapPhase:
			if len(c.inProgressTasks) == 0 {
				panic("coordinator: empty in-progress tasks during map phase")
			}
			return types.Task{}, ErrWait
		case reducePhase:
			return types.Task{}, ErrWait
		default:
			panic("coordinator: invalid phase reached")
		}
	}

	var kind types.TaskKind
	switch c.phase {
	case mapPhase:
		kind = types.MapTask
	case reducePhase:
		kind = types.ReduceTask
	}

	t.workerID = workerID
	c.inProgressTasks[t.id] = t

	return types.Task{
		ID:    t.id,
		Kind:  kind,
		Files: t.files,
	}, nil
}

// ReportCompletion marks a task as completed and advances the job state.
//
// When all map tasks complete, reduce tasks are enqueued and the coordinator
// transitions to the reduce phase. When all reduce tasks complete, the job is
// marked as done.
func (c *Coordinator) ReportCompletion(taskID int) error {
	t, ok := c.inProgressTasks[taskID]
	if !ok {
		return fmt.Errorf("task %d not in progress", taskID)
	}

	c.completedTasks = append(c.completedTasks, t)
	delete(c.inProgressTasks, t.id)

	if len(c.pendingTasks.tasks) == 0 && len(c.inProgressTasks) == 0 {
		if c.phase == reducePhase {
			c.phase = donePhase
			return nil
		}

		for i := range c.numReducers {
			var files []string
			for tn := range len(c.completedTasks) {
				files = append(files, fmt.Sprintf("inter-%d-%d", tn, i))
			}
			c.pendingTasks.enqueue(taskRecord{id: i, files: files})
		}
		c.phase = reducePhase
	}
	return nil
}
