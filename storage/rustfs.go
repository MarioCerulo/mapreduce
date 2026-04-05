package storage

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/MarioCerulo/mapreduce/engine/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type RustFS struct {
	client *s3.Client
	bucket string
}

func NewRustFS(bucketName string) (*RustFS, error) {
	region := os.Getenv("RUSTFS_REGION")
	accessKeyID := os.Getenv("RUSTFS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("RUSTFS_SECRET_ACCESS_KEY")
	endpoint := os.Getenv("RUSTFS_ENDPOINT_URL")

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				accessKeyID,
				secretAccessKey,
				"",
			),
		),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	return &RustFS{
		client: client,
		bucket: bucketName,
	}, nil
}

func (s *RustFS) LoadInputFile(fileName string) (string, error) {
	resp, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fileName),
	})

	if err != nil {
		return "", err
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (s *RustFS) LoadIntermediateFile(fileName string) ([]types.KeyValue, error) {
	resp, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fileName),
	})

	if err != nil {
		return nil, err
	}

	var kvs []types.KeyValue
	scanner := bufio.NewScanner(resp.Body)
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

func (s *RustFS) Save(fileName string, kvs []types.KeyValue) error {
	var buf bytes.Buffer
	for _, kv := range kvs {
		fmt.Fprintf(&buf, "%s %s\n", kv.Key, kv.Value)
	}
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(fileName),
		Body:          bytes.NewReader(buf.Bytes()),
		ContentLength: aws.Int64(int64(len(buf.Bytes()))),
	})
	return err
}
