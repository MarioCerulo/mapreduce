package main

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/MarioCerulo/mapreduce/engine"
	"github.com/MarioCerulo/mapreduce/engine/types"
)

type WordCountJob struct{}

func (WordCountJob) Map(key, value string) []types.KeyValue {
	var kv []types.KeyValue
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
		kv = append(kv, types.KeyValue{Key: word, Value: "1"})
	}
	return kv
}

func (WordCountJob) Reduce(key string, vals []string) string {
	return strconv.Itoa(len(vals))
}

func main() {
	res, err := engine.MapReduce("input.txt", WordCountJob{})
	if err != nil {
		panic(err)
	}
	for _, kv := range res {
		fmt.Printf("%s %s\n", kv.Key, kv.Value)
	}
}
