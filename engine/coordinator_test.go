package engine_test

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/MarioCerulo/mapreduce/engine"
	"github.com/MarioCerulo/mapreduce/engine/types"
)

func TestCoordinator(t *testing.T) {
	t.Run("complete workflow happy path", func(t *testing.T) {
		c, err := engine.NewCoordinator([]string{"input.txt"}, 1)
		if err != nil {
			t.Fatal(err)
		}
		go c.Run(t.Context())

		task, err := c.RequestTask("worker")
		if err != nil {
			t.Fatal(err)
		}

		expected := types.Task{ID: 0, Kind: types.MapTask, NReducers: 1, Files: []string{"input.txt"}}
		if !reflect.DeepEqual(task, expected) {
			t.Fatalf("expected %+v, got %+v", expected, task)
		}

		if err := c.ReportCompletion(task.ID); err != nil {
			t.Fatal(err)
		}

		task, err = c.RequestTask("worker")
		if err != nil {
			t.Fatal(err)
		}
		if task.Kind != types.ReduceTask {
			t.Fatal("Expected a reduce task, got a map task")
		}

		if err := c.ReportCompletion(task.ID); err != nil {
			t.Fatal(err)
		}

		if _, err := c.RequestTask("worker"); !errors.Is(err, engine.ErrDone) {
			t.Fatalf("Expected {%v}, got {%v}", engine.ErrDone, err)
		}
	})

	t.Run("multiple chunks and reducers", func(t *testing.T) {
		c, err := engine.NewCoordinator([]string{"input-0.txt", "input-1.txt"}, 2)
		if err != nil {
			t.Fatal(err)
		}
		go c.Run(t.Context())

		mapT1, err := c.RequestTask("map-worker")
		if err != nil {
			t.Fatal(err)
		}

		if mapT1.Kind != types.MapTask {
			t.Fatalf("Expected a map task, got a reduce task: %+v", mapT1)
		}

		if err := c.ReportCompletion(mapT1.ID); err != nil {
			t.Fatal(err)
		}

		mapT2, err := c.RequestTask("map-worker")
		if err != nil {
			t.Fatal(err)
		}

		if mapT2.Kind != types.MapTask {
			t.Fatalf("Expected a map task, got a reduce task: %+v", mapT1)
		}

		if err := c.ReportCompletion(mapT2.ID); err != nil {
			t.Fatal(err)
		}

		expected := []string{"inter-0-0", "inter-1-0", "inter-0-1", "inter-1-1"}
		slices.Sort(expected)
		var returned []string

		reduceT1, err := c.RequestTask("reduce-worker")
		if err != nil {
			t.Fatal(err)
		}

		if reduceT1.Kind != types.ReduceTask {
			t.Fatalf("Expected a reduce task, got a map task: %+v", reduceT1)
		}
		returned = append(returned, reduceT1.Files...)

		reduceT2, err := c.RequestTask("reduce-worker")
		if err != nil {
			t.Fatal(err)
		}

		if reduceT2.Kind != types.ReduceTask {
			t.Fatalf("Expected a reduce task, got a map task: %+v", reduceT1)
		}

		returned = append(returned, reduceT2.Files...)

		slices.Sort(returned)
		if !reflect.DeepEqual(expected, returned) {
			t.Fatalf("expected %+v, got %+v", expected, returned)
		}

		if err := c.ReportCompletion(reduceT1.ID); err != nil {
			t.Fatal(err)
		}

		if err := c.ReportCompletion(reduceT2.ID); err != nil {
			t.Fatal(err)
		}

		if _, err := c.RequestTask("worker"); err == nil || !errors.Is(err, engine.ErrDone) {
			t.Fatalf("expected {%v}, got {%v}", engine.ErrDone, err)
		}
	})

	t.Run("out of order completion", func(t *testing.T) {
		c, err := engine.NewCoordinator([]string{"chunk-0.txt", "chunk-1.txt"}, 1)
		if err != nil {
			t.Fatal(err)
		}
		go c.Run(t.Context())

		mapT1, err := c.RequestTask("worker-0")
		if err != nil {
			t.Fatal(err)
		}

		mapT2, err := c.RequestTask("worker-1")
		if err != nil {
			t.Fatal(err)
		}

		if err := c.ReportCompletion(mapT2.ID); err != nil {
			t.Fatal(err)
		}

		if err := c.ReportCompletion(mapT1.ID); err != nil {
			t.Fatal(err)
		}

		reduceT, err := c.RequestTask("worker")
		if err != nil {
			t.Fatal(err)
		}

		if reduceT.Kind != types.ReduceTask {
			t.Fatal("Expected a reduce task, got a map task")
		}

		expected := []string{"inter-0-0", "inter-1-0"}
		slices.Sort(reduceT.Files)
		slices.Sort(expected)

		if !reflect.DeepEqual(reduceT.Files, expected) {
			t.Fatalf("expected %v, got %v", expected, reduceT.Files)
		}
	})

	t.Run("empty input slice", func(t *testing.T) {
		_, err := engine.NewCoordinator([]string{}, 1)
		if err == nil {
			t.Fatal("expected an error, got <nil>")
		}
	})

	t.Run("less than one reducer", func(t *testing.T) {
		input := []string{"input.txt"}
		_, err := engine.NewCoordinator(input, 0)
		if err == nil {
			t.Fatal("expected an error, got <nil>")
		}

		_, err = engine.NewCoordinator(input, -1)
		if err == nil {
			t.Fatal("expected an error, got <nil>")
		}
	})

	t.Run("return wait on empty pending task list", func(t *testing.T) {
		c, err := engine.NewCoordinator([]string{"input.txt"}, 1)
		if err != nil {
			t.Fatal(err)
		}
		go c.Run(t.Context())

		task, err := c.RequestTask("worker-0")
		if err != nil {
			t.Fatal(err)
		}

		// Check for Wait returned for map tasks
		if _, err := c.RequestTask("worker-1"); !errors.Is(err, engine.ErrWait) {
			t.Fatalf("expected {%v}, got {%v}", engine.ErrWait, err)
		}

		if err := c.ReportCompletion(task.ID); err != nil {
			t.Fatal(err)
		}

		// Check for Wait returned for reduce tasks
		_, err = c.RequestTask("worker-0")
		if err != nil {
			t.Fatal(err)
		}

		if _, err := c.RequestTask("worker-1"); !errors.Is(err, engine.ErrWait) {
			t.Fatalf("Expected {%v}, got {%v}", engine.ErrWait, err)
		}
	})

	t.Run("return error on wrong task id report", func(t *testing.T) {
		c, err := engine.NewCoordinator([]string{"input.txt"}, 1)
		if err != nil {
			t.Fatal(err)
		}
		go c.Run(t.Context())

		c.RequestTask("worker")
		if err := c.ReportCompletion(3); err == nil {
			t.Fatal("Expected to get an error")
		}
	})
}
