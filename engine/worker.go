package engine

type taskType int

const (
	mapTask taskType = iota
	reduceTask
)

type task struct {
	kind       taskType
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

func (w worker) run(task task) []KeyValue {
	var res []KeyValue
	switch task.kind {
	case mapTask:
		res = w.job.Map(task.key, task.mapVal)

	case reduceTask:
		res = []KeyValue{
			{Key: task.key, Value: w.job.Reduce(task.key, task.reduceVals)},
		}
	}
	return res
}
