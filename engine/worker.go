package engine

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"slices"
	"time"

	"github.com/MarioCerulo/mapreduce/engine/types"
)

// CoordinatorClient is the interface through which a Worker communicates with the coordinator.
type CoordinatorClient interface {
	RequestTask(ctx context.Context, workerID string) (types.Task, error)
	ReportCompletion(ctx context.Context, taskID int) error
	Heartbeat(ctx context.Context, workerID string) error
}

// Storage abstracts file I/O for input, intermediate, and output data.
// Implementation may target local disk, object storage, or any other backend.
type Storage interface {
	LoadInputFile(ctx context.Context, filePath string) (string, error)
	LoadIntermediateFile(ctx context.Context, filePath string) ([]types.KeyValue, error)
	Save(ctx context.Context, filePath string, content []types.KeyValue) error
}

// Worker executes map and reduce tasks assigned by the coordinator.
type Worker struct {
	id     string
	job    Job
	ttl    time.Duration
	logger *slog.Logger
}

func partition(key string, nReducers int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return (int(h.Sum32()) & 0x7fffffff) % nReducers
}

// NewWorker creates a Worker with a randomly generated ID.
// ttl controls how ofter heartbeats are sent to the coordinator.
func NewWorker(job Job, ttl time.Duration, logger *slog.Logger) Worker {
	b := make([]byte, 32)
	rand.Read(b)
	id := hex.EncodeToString(b)

	logger = logger.With(slog.String("component", "worker"), slog.String("worker_id", shortID(id)))

	return Worker{
		id:     id,
		job:    job,
		ttl:    ttl,
		logger: logger,
	}
}

// Run starts the worker's task loop, polling the coordinator for tasks until
// the job is complete or ctx is cancelled. A background goroutine sends periodic
// heartbeats to the coordinator at the configured TTL interval.
func (w Worker) Run(ctx context.Context, client CoordinatorClient, store Storage) error {
	w.logger.Info("worker_started")

	go func(ctx context.Context) {
		ticker := time.NewTicker(w.ttl)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := client.Heartbeat(ctx, w.id); err != nil {
					w.logger.Warn("heartbeat_failed", slog.String("error", err.Error()))
				}
			}
		}
	}(ctx)

	for {
		task, err := client.RequestTask(ctx, w.id)
		if err != nil {
			if errors.Is(err, ErrWait) {
				w.logger.Debug("wait_received")
				select {
				case <-time.After(500 * time.Millisecond):
				case <-ctx.Done():
					return ctx.Err()
				}
				continue
			}
			if errors.Is(err, ErrDone) {
				w.logger.Debug("done_received")
				return nil
			}
			return err
		}
		w.logger.Debug("task_received",
			slog.Int("task_id", task.ID),
			slog.String("task_type", task.Kind.String()),
		)

		switch task.Kind {
		case types.MapTask:
			content, err := store.LoadInputFile(ctx, task.Files[0])
			if err != nil {
				return err
			}
			res := w.job.Map(task.Files[0], content)

			buckets := make(map[int][]types.KeyValue)
			for _, kv := range res {
				b := partition(kv.Key, task.NReducers)
				buckets[b] = append(buckets[b], kv)
			}

			for bucket, kvs := range buckets {
				if err := store.Save(ctx, fmt.Sprintf("inter-%d-%d", task.ID, bucket), kvs); err != nil {
					return err
				}
			}

			if err := client.ReportCompletion(ctx, task.ID); err != nil {
				return err
			}

		case types.ReduceTask:
			content := make([]types.KeyValue, 0, len(task.Files))
			for _, file := range task.Files {
				kvs, err := store.LoadIntermediateFile(ctx, file)
				if err != nil {
					return err
				}
				content = append(content, kvs...)
			}

			slices.SortFunc(content, func(a, b types.KeyValue) int {
				return cmp.Compare(a.Key, b.Key)
			})

			var res []types.KeyValue
			i := 0
			for i < len(content) {
				current := content[i].Key
				var vals []string
				for i < len(content) && current == content[i].Key {
					vals = append(vals, content[i].Value)
					i++
				}
				res = append(res, types.KeyValue{Key: current, Value: w.job.Reduce(current, vals)})
			}

			if err := store.Save(ctx, fmt.Sprintf("mr-%d", task.ID), res); err != nil {
				return err
			}

			if err := client.ReportCompletion(ctx, task.ID); err != nil {
				return err
			}
		}

		w.logger.Debug("task_completed",
			slog.Int("task_id", task.ID),
			slog.String("task_type", task.Kind.String()),
		)

	}
}
