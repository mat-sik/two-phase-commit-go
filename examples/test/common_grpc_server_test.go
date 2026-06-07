package test

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	basic "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/basic/v1"
	transfer "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/transfer/v1"
	"google.golang.org/grpc"
)

func basicGRPCServerRequests(handlers []*client.GRPCBasicHandler) []runServerRequest {
	return mapToRunServerRequests(handlers, mapFromBasicHandler)
}

func mapFromBasicHandler(handler *client.GRPCBasicHandler) runServerRequest {
	registerer := func(server *grpc.Server) {
		basic.RegisterBasicServiceServer(server, handler)
	}
	return newRunServerRequest(registerer)
}

func transferGRPCServerRequests(handlers []*client.GRPCTransferHandler) []runServerRequest {
	return mapToRunServerRequests(handlers, mapFromTransferHandler)
}

func mapFromTransferHandler(handler *client.GRPCTransferHandler) runServerRequest {
	registerer := func(server *grpc.Server) {
		transfer.RegisterTransferServiceServer(server, handler)
	}
	return newRunServerRequest(registerer)
}

func newRunServerRequest(registerer client.GRPCHandlerRegisterer) runServerRequest {
	var serverPtr atomic.Pointer[grpc.Server]
	return runServerRequest{
		serverRunner:  newGRPCServerRunner(registerer, &serverPtr),
		serverStopper: newGRPCServerStopper(&serverPtr),
	}
}

func newGRPCServerRunner(registerer client.GRPCHandlerRegisterer, ref *atomic.Pointer[grpc.Server]) serverRunner {
	return func(wg *sync.WaitGroup, errCh chan<- error, lis net.Listener) {
		srv := client.NewGRPCServer(registerer, lis.Addr())
		ref.Store(srv)
		runServer(wg, errCh, func() error {
			return srv.Serve(lis)
		})
	}
}

func newGRPCServerStopper(ref *atomic.Pointer[grpc.Server]) serverStopper {
	return func() error {
		srv := ref.Load()
		if srv == nil {
			panic("server runner need to run before calling associated server stopper")
		}
		srv.GracefulStop()
		return nil
	}
}
