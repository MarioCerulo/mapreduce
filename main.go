package main

import (
	"cmp"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"unicode"
)

type KeyValue struct {
	Key, Value string
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

type Task struct {
	taskType   TaskType
	Key        string
	MapVal     string
	ReduceVals []string
}

type Job interface {
	Map(string, string) []KeyValue
	Reduce(string, []string) string
}

type WordCountJob struct{}

func (WordCountJob) Map(key, value string) []KeyValue {
	var kv []KeyValue
	for word := range strings.FieldsSeq(value) {
		word = strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsNumber(r) {
				return r
			}
			return -1
		}, word)
		if word == "" {
			continue
		}
		word = strings.ToLower(word)
		kv = append(kv, KeyValue{Key: word, Value: "1"})
	}
	return kv
}

func (WordCountJob) Reduce(key string, vals []string) string {
	return strconv.Itoa(len(vals))
}

type Worker struct {
	job Job
}

func NewWorker(job Job) Worker {
	return Worker{
		job: job,
	}
}

func (w Worker) Run(task Task) []KeyValue {
	var res []KeyValue
	switch task.taskType {
	case MapTask:
		res = w.job.Map(task.Key, task.MapVal)

	case ReduceTask:
		res = []KeyValue{
			{Key: task.Key, Value: w.job.Reduce(task.Key, task.ReduceVals)},
		}
	}
	return res
}

func main() {
	file, err := os.Open("input.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	inputText, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}

	job := WordCountJob{}
	w := NewWorker(job)
	mapped := w.Run(Task{taskType: MapTask, Key: "input.txt", MapVal: string(inputText)})

	slices.SortFunc(mapped, func(a, b KeyValue) int {
		return cmp.Compare(a.Key, b.Key)
	})

	var res []KeyValue
	for i := 0; i < len(mapped); {
		key := mapped[i].Key
		var vals []string
		for i < len(mapped) && mapped[i].Key == key {
			vals = append(vals, mapped[i].Value)
			i++
		}

		res = append(res, w.Run(Task{
			taskType:   ReduceTask,
			Key:        key,
			ReduceVals: vals,
		})[0])
	}

	for _, kv := range res {
		fmt.Printf("%s %s\n", kv.Key, kv.Value)
	}
}
