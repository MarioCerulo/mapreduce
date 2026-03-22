package engine

import "github.com/MarioCerulo/mapreduce/engine/types"

type task struct {
	kind       types.TaskKind
	key        string
	mapVal     string
	reduceVals []string
}

type worker struct {
	job Job
}

func newWorker(job Job) worker {
	return worker{
		job: job,
	}
}

func (w worker) run(task task) []types.KeyValue {
	var res []types.KeyValue
	switch task.kind {
	case types.MapTask:
		res = w.job.Map(task.key, task.mapVal)

	case types.ReduceTask:
		res = []types.KeyValue{
			{Key: task.key, Value: w.job.Reduce(task.key, task.reduceVals)},
		}
	}
	return res
}
