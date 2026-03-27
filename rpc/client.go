package rpc

import (
	"context"

	"github.com/MarioCerulo/mapreduce/engine"
	"github.com/MarioCerulo/mapreduce/engine/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	client CoordinatorClient
}

func NewClient(serverAddr string) (*Client, error) {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := NewCoordinatorClient(conn)
	return &Client{conn: conn, client: c}, nil
}

func (c *Client) RequestTask(ctx context.Context, workerID string) (types.Task, error) {
	res, err := c.client.RequestTask(ctx, &TaskRequest{WorkerId: workerID})
	if err != nil {
		return types.Task{}, err
	}

	switch res.Type {
	case TaskType_TASKTYPE_DONE:
		return types.Task{}, engine.ErrDone

	case TaskType_TASKTYPE_WAIT:
		return types.Task{}, engine.ErrWait

	case TaskType_TASKTYPE_MAP:
		return types.Task{
			ID:        int(res.TaskId),
			Kind:      types.MapTask,
			NReducers: int(*res.NReducers),
			Files:     res.Files,
		}, nil

	case TaskType_TASKTYPE_REDUCE:
		return types.Task{
			ID:    int(res.TaskId),
			Kind:  types.ReduceTask,
			Files: res.Files,
		}, nil
	}

	panic("unreachable")
}

func (c *Client) ReportCompletion(ctx context.Context, taskID int) error {
	_, err := c.client.ReportCompletion(ctx, &Report{TaskId: int32(taskID)})
	return err
}

func (c *Client) Close() error {
	return c.conn.Close()
}
