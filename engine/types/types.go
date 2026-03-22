package types

// KeyValue is the fundamental unit of data in a MapReduce computation.
type KeyValue struct {
	Key, Value string
}

// TaskKind identifies whether a task is a map or reduce operation.
type TaskKind int

const (
	// process an input chunk
	MapTask TaskKind = iota
	// aggregate intermediate results
	ReduceTask
)

// Task is a single unit of work assigned to a worker by the coordinator.
type Task struct {
	ID   int
	Kind TaskKind
	// Files contains the input chunk path for map tasks,
	// or intermediate file paths for reduce tasks.
	Files []string
}
