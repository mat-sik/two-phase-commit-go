package client

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	pb "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/basic/v1"
	"google.golang.org/grpc"
)

func GRPCServerRequests(handlers []*GRPCHandler) []RunServerRequest {
	return mapToRunServerRequests(handlers, mapToGRPCServerRequest)
}

func mapToGRPCServerRequest(handler *GRPCHandler) RunServerRequest {
	registerer := func(server *grpc.Server) {
		pb.RegisterBasicServiceServer(server, handler)
	}

	var serverPtr atomic.Pointer[grpc.Server]
	return RunServerRequest{
		serverRunner:  newGRPCServerRunner(registerer, &serverPtr),
		serverStopper: newGRPCServerStopper(&serverPtr),
	}
}

func newGRPCServerRunner(registerer gRPCHandlerRegisterer, ref *atomic.Pointer[grpc.Server]) ServerRunner {
	return func(wg *sync.WaitGroup, errCh chan<- error, lis net.Listener) {
		srv := newGRPCServer(registerer, lis.Addr())
		ref.Store(srv)
		runServer(wg, errCh, func() error {
			return srv.Serve(lis)
		})
	}
}

func newGRPCServer(registerer gRPCHandlerRegisterer, addr net.Addr) *grpc.Server {
	server := grpc.NewServer(
		grpc.UnaryInterceptor(serverAddrInterceptor(addr)),
	)
	registerer(server)
	return server
}

type gRPCHandlerRegisterer func(server *grpc.Server)

func serverAddrInterceptor(addr net.Addr) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(contextWithAddress(ctx, addr), req)
	}
}

func newGRPCServerStopper(ref *atomic.Pointer[grpc.Server]) ServerStopper {
	return func() error {
		srv := ref.Load()
		if srv == nil {
			panic("server runner need to run before calling associated server stopper")
		}
		srv.GracefulStop()
		return nil
	}
}
