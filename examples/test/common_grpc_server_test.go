package test

import (
	"net"
	"sync"
	"sync/atomic"

	basic "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/basic/v1"
	transfer "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/transfer/v1"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/adapter"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/server"
	"google.golang.org/grpc"
)

func basicGRPCServerLaunches(handlers []*adapter.GRPCBasicHandler) []serverLaunch {
	return mapToServerLaunches(handlers, mapFromGRPCBasicHandler)
}

func mapFromGRPCBasicHandler(handler *adapter.GRPCBasicHandler) serverLaunch {
	registerer := func(server *grpc.Server) {
		basic.RegisterBasicServiceServer(server, handler)
	}
	return newServerLaunch(registerer)
}

func mapFromGRPCTransferHandler(handler *adapter.GRPCTransferHandler) serverLaunch {
	registerer := func(server *grpc.Server) {
		transfer.RegisterTransferServiceServer(server, handler)
	}
	return newServerLaunch(registerer)
}

func newServerLaunch(registerer server.GRPCHandlerRegisterer) serverLaunch {
	var serverPtr atomic.Pointer[grpc.Server]
	return serverLaunch{
		serverRunner:  newGRPCServerRunner(registerer, &serverPtr),
		serverStopper: newGRPCServerStopper(&serverPtr),
	}
}

func newGRPCServerRunner(registerer server.GRPCHandlerRegisterer, ref *atomic.Pointer[grpc.Server]) serverRunner {
	return func(wg *sync.WaitGroup, errCh chan<- error, lis net.Listener) {
		srv := server.NewGRPCServer(registerer, lis.Addr())
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
