package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"unicode"

	"github.com/joho/godotenv"

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
	godotenv.Load()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	c, err := rpc.NewClient("127.0.0.1:50051")
	if err != nil {
		logger.Error("failed to create RPC client", slog.Any("err", err))
		os.Exit(1)
	}
	defer c.Close()

	w := engine.NewWorker(WordCountJob{}, logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := storage.NewRustFS("mapreduce")
	if err != nil {
		logger.Error("failed to create file store", slog.Any("err", err))
		os.Exit(1)
	}

	if err := w.Run(ctx, c, store); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("worker exited with error", slog.Any("err", err))
		os.Exit(1)
	}
}
