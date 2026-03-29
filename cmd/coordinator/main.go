package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/MarioCerulo/mapreduce/engine"
	pb "github.com/MarioCerulo/mapreduce/rpc"
	"google.golang.org/grpc"
)

func main() {
	input := []string{"input.txt"}
	nReducers := flag.Int("reducers", 1, "Number of reducers")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	c, err := engine.NewCoordinator(input, *nReducers, logger)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go c.Run(ctx)

	addr := ":50051"
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	coord := pb.NewServer(c)

	// handle graceful shutdown
	go func() {
		<-ctx.Done()
		server.GracefulStop()
		logger.Info("server_stopped")
	}()

	pb.RegisterCoordinatorServer(server, coord)

	logger.Info("server_started", slog.String("addr", addr))
	if err := server.Serve(lis); err != nil {
		logger.Error("server_error", slog.Any("err", err))
	}
}
