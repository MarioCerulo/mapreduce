package engine_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MarioCerulo/mapreduce/engine"
)

type UppercaseJob struct{}

func (UppercaseJob) Map(key, val string) []engine.KeyValue {
	return []engine.KeyValue{
		{Key: key, Value: strings.ToUpper(val)},
	}
}

func (UppercaseJob) Reduce(key string, vals []string) string {
	return vals[0]
}

func TestMapReduce(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "text.txt")
	if err := os.WriteFile(path, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	res, err := engine.MapReduce(path, UppercaseJob{})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Value != "TEST" {
		t.Fatalf("expected `TEST`, got %s", res[0].Value)
	}
}
