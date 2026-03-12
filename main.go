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

func Map(key, value string) []KeyValue {
	var kv []KeyValue
	for word := range strings.FieldsSeq(value) {
		word = strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) {
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

func Reduce(key string, vals []string) string {
	return strconv.Itoa(len(vals))
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

	mapped := Map("input.txt", string(inputText))

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

		res = append(res, KeyValue{Key: key, Value: Reduce(key, vals)})
	}

	for _, kv := range res {
		fmt.Printf("%s %s\n", kv.Key, kv.Value)
	}
}
