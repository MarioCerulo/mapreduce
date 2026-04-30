package rpc

import (
	"context"
	"errors"

	"github.com/MarioCerulo/mapreduce/engine"
	"github.com/MarioCerulo/mapreduce/engine/types"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Server bridges incoming gRPC calls to an [engine.Coordinator].
type Server struct {
	UnimplementedCoordinatorServer
	coordinator *engine.Coordinator
}

// NewServer wraps c as a gRPC-compatible coordinator server.
func NewServer(c *engine.Coordinator) *Server {
	return &Server{
		coordinator: c,
	}
}

func (s *Server) RequestTask(_ context.Context, req *TaskRequest) (*TaskResponse, error) {
	task, err := s.coordinator.RequestTask(req.WorkerId)
	if err != nil {
		switch {
		case errors.Is(err, engine.ErrWait):
			return &TaskResponse{
				Type: TaskType_TASKTYPE_WAIT,
			}, nil
		case errors.Is(err, engine.ErrDone):
			return &TaskResponse{
				Type: TaskType_TASKTYPE_DONE,
			}, nil
		default:
			return nil, err
		}
	}

	switch task.Kind {
	case types.MapTask:
		return &TaskResponse{
			TaskId:    int32(task.ID),
			NReducers: new(int32(task.NReducers)),
			Type:      TaskType_TASKTYPE_MAP,
			Files:     task.Files,
		}, nil
	case types.ReduceTask:
		return &TaskResponse{
			TaskId: int32(task.ID),
			Type:   TaskType_TASKTYPE_REDUCE,
			Files:  task.Files,
		}, nil
	}

	panic("unreachable")
}

func (s *Server) ReportCompletion(_ context.Context, report *Report) (*Ack, error) {
	if err := s.coordinator.ReportCompletion(int(report.TaskId)); err != nil {
		return nil, err
	}
	return &Ack{}, nil
}

func (s *Server) Heartbeat(_ context.Context, workerID *WorkerId) (*emptypb.Empty, error) {
	s.coordinator.Heartbeat(workerID.WorkerId)
	return &emptypb.Empty{}, nil
}
