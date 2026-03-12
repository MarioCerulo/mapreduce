package engine

type KeyValue struct {
	Key, Value string
}

type Job interface {
	Map(string, string) []KeyValue
	Reduce(string, []string) string
}
