package engine

import "github.com/MarioCerulo/mapreduce/engine/types"

// Job defines the user-supplied logic for a MapReduce computation.
//
// Map is called once per input chunk and emits intermediate key-value pairs.
// Reduce is called once per unique key with all associated values and returns a single result.
type Job interface {
	Map(key string, val string) []types.KeyValue
	Reduce(key string, vals []string) string
}
