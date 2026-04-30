package engine_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/MarioCerulo/mapreduce/engine"
	"github.com/MarioCerulo/mapreduce/engine/types"
)

type testClient struct {
	tasks     []types.Task
	completed int
}

func (t *testClient) RequestTask(_ context.Context, _ string) (types.Task, error) {
	if len(t.tasks) == 0 {
		return types.Task{}, engine.ErrDone
	}
	task := t.tasks[0]
	t.tasks = t.tasks[1:]
	return task, nil
}

func (t *testClient) ReportCompletion(_ context.Context, taskID int) error {
	t.completed++
	return nil
}

func (t *testClient) Heartbeat(_ context.Context, workerID string) error {
	return nil
}

type testStorage struct {
	input string
	store map[string][]types.KeyValue
}

func (s *testStorage) LoadInputFile(_ context.Context, filePath string) (string, error) {
	return s.input, nil
}

func (s *testStorage) LoadIntermediateFile(_ context.Context, filePath string) ([]types.KeyValue, error) {
	return s.store[filePath], nil
}

func (s *testStorage) Save(_ context.Context, filePath string, content []types.KeyValue) error {
	s.store[filePath] = append(s.store[filePath], content...)
	return nil
}

type UppercaseJob struct{}

func (UppercaseJob) Map(key, val string) []types.KeyValue {
	return []types.KeyValue{
		{Key: key, Value: strings.ToUpper(val)},
	}
}

func (UppercaseJob) Reduce(key string, vals []string) string {
	return vals[0]
}

const ttl = time.Second

func TestWorker(t *testing.T) {
	t.Run("map-reduce workflow completed", func(t *testing.T) {
		c := &testClient{
			tasks: []types.Task{
				{
					ID:        0,
					Kind:      types.MapTask,
					NReducers: 1,
					Files:     []string{"input.txt"},
				},
				{
					ID:    1,
					Kind:  types.ReduceTask,
					Files: []string{"inter-0-0"},
				},
			},
		}

		s := &testStorage{
			input: "test",
			store: make(map[string][]types.KeyValue),
		}

		worker := engine.NewWorker(UppercaseJob{}, ttl, newTestLogger())

		if err := worker.Run(t.Context(), c, s); err != nil {
			t.Fatal(err)
		}
		kv, ok := s.store["inter-0-0"]
		if !ok {
			t.Fatal("intermediate file not found")
		}
		val := kv[0].Value
		if val != "TEST" {
			t.Fatalf("expected TEST, got %s", val)
		}

		kv, ok = s.store["mr-1"]
		if !ok {
			t.Fatal("final file not found")
		}

		val = kv[0].Value
		if val != "TEST" {
			t.Fatalf("expected TEST, got %s", val)
		}

		if c.completed != 2 {
			t.Fatalf("Expected 2 completions, got %d", c.completed)
		}
	})
}
