package client

import (
	"net"
	"sync"

	pb "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/client/v1"
	"google.golang.org/grpc"
)

func GRPCServerRequests(handlers []*GRPCHandler) []RunServerRequest {
	return mapToRunServerRequests(handlers, mapToGRPCServerRequest)
}

func mapToGRPCServerRequest(handler *GRPCHandler) RunServerRequest {
	srv := newGRPCServer(handler)
	return RunServerRequest{
		serverRunner:  newGRPCServerRunner(srv),
		serverStopper: newGRPCServerStopper(srv),
	}
}

func newGRPCServer(handler *GRPCHandler) *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterClientServiceServer(server, handler)
	return server
}

func newGRPCServerRunner(srv *grpc.Server) ServerRunner {
	return func(wg *sync.WaitGroup, errCh chan<- error, lis net.Listener) {
		runServer(wg, errCh, func() error {
			return srv.Serve(lis)
		})
	}
}

func newGRPCServerStopper(srv *grpc.Server) ServerStopper {
	return func() error {
		srv.GracefulStop()
		return nil
	}
}
