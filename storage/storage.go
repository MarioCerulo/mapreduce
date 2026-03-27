package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/MarioCerulo/mapreduce/engine/types"
)

type Storage struct {
	basePath string
}

func NewStorage(basePath string) Storage {
	return Storage{
		basePath: basePath,
	}
}

func (s Storage) LoadInputFile(fileName string) (string, error) {
	fileName = filepath.Join(s.basePath, fileName)
	content, err := os.ReadFile(fileName)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func (s Storage) LoadIntermediateFile(fileName string) ([]types.KeyValue, error) {
	fileName = filepath.Join(s.basePath, fileName)
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var kvs []types.KeyValue
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		pair := strings.SplitN(line, " ", 2)

		if len(pair) != 2 {
			return nil, fmt.Errorf("malformed intermediate file: %s - line %q", fileName, line)
		}
		kvs = append(kvs, types.KeyValue{Key: pair[0], Value: pair[1]})
	}

	return kvs, scanner.Err()
}

func (s Storage) Save(fileName string, kvs []types.KeyValue) error {
	fileName = filepath.Join(s.basePath, fileName)
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, kv := range kvs {
		if _, err := writer.WriteString(kv.Key + " " + kv.Value + "\n"); err != nil {
			return err
		}
	}
	return writer.Flush()
}
