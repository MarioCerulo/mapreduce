package engine

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"time"

	"github.com/MarioCerulo/mapreduce/engine/types"
)

type CoordinatorClient interface {
	RequestTask(ctx context.Context, workerID string) (types.Task, error)
	ReportCompletion(ctx context.Context, taskID int) error
}

type Storage interface {
	LoadInputFile(filePath string) (string, error)
	LoadIntermediateFile(filePath string) ([]types.KeyValue, error)
	Save(filePath string, content []types.KeyValue) error
}

type Worker struct {
	id  string
	job Job
}

func partition(key string, nReducers int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return (int(h.Sum32()) & 0x7fffffff) % nReducers
}

func NewWorker(job Job) Worker {
	b := make([]byte, 32)
	rand.Read(b)

	return Worker{
		id:  hex.EncodeToString(b),
		job: job,
	}
}

func (w Worker) Run(ctx context.Context, client CoordinatorClient, store Storage) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			task, err := client.RequestTask(ctx, w.id)
			if err != nil {
				if errors.Is(err, ErrWait) {
					time.Sleep(500 * time.Millisecond)
					continue
				}
				if errors.Is(err, ErrDone) {
					return nil
				}
				return err
			}

			switch task.Kind {
			case types.MapTask:
				content, err := store.LoadInputFile(task.Files[0])
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
					if err := store.Save(fmt.Sprintf("inter-%d-%d", task.ID, bucket), kvs); err != nil {
						return err
					}
				}

				if err := client.ReportCompletion(ctx, task.ID); err != nil {
					return err
				}

			case types.ReduceTask:
				content := make([]types.KeyValue, 0, len(task.Files))
				for _, file := range task.Files {
					kvs, err := store.LoadIntermediateFile(file)
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

				if err := store.Save(fmt.Sprintf("mr-%d", task.ID), res); err != nil {
					return err
				}

				if err := client.ReportCompletion(ctx, task.ID); err != nil {
					return err
				}
			}
		}
	}
}
