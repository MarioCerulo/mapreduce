package engine

import (
	"cmp"
	"io"
	"os"
	"slices"

	"github.com/MarioCerulo/mapreduce/engine/types"
)

func MapReduce(inputFile string, job Job) ([]types.KeyValue, error) {
	file, err := os.Open(inputFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	inputText, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	w := newWorker(job)
	mapped := w.run(task{kind: types.MapTask, key: inputFile, mapVal: string(inputText)})

	slices.SortFunc(mapped, func(a, b types.KeyValue) int {
		return cmp.Compare(a.Key, b.Key)
	})

	var res []types.KeyValue
	for i := 0; i < len(mapped); {
		key := mapped[i].Key
		var vals []string
		for i < len(mapped) && mapped[i].Key == key {
			vals = append(vals, mapped[i].Value)
			i++
		}

		res = append(res, w.run(task{
			kind:       types.ReduceTask,
			key:        key,
			reduceVals: vals,
		})[0])
	}

	return res, nil
}
