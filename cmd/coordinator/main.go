package main

import (
	"context"
	"flag"
	"log"
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

	c, err := engine.NewCoordinator(input, *nReducers)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go c.Run(ctx)

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
	}()

	pb.RegisterCoordinatorServer(server, coord)

	if err := server.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
