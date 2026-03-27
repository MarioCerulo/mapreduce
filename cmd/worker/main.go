package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"unicode"

	"github.com/MarioCerulo/mapreduce/engine"
	"github.com/MarioCerulo/mapreduce/engine/types"
	"github.com/MarioCerulo/mapreduce/rpc"
	"github.com/MarioCerulo/mapreduce/storage"
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
	c, err := rpc.NewClient("127.0.0.1:50051")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	w := engine.NewWorker(WordCountJob{})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := w.Run(ctx, c, storage.NewStorage("store")); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
