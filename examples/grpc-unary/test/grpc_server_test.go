package test

import (
	"fmt"
	"net"
	"sync"

	"github.com/mat-sik/two-phase-commit-go/examples/grpc-unary/internal/client"
	pb "github.com/mat-sik/two-phase-commit-go/examples/grpc-unary/internal/generated/client/v1"
	"google.golang.org/grpc"
)

func runServers(requests []serverConfig) (serverBundle, error) {
	listeners := make([]net.Listener, 0, len(requests))
	for _, req := range requests {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", req.port))
		if err != nil {
			return serverBundle{}, err
		}
		listeners = append(listeners, lis)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(requests))

	serverErrsChan := make(chan error, len(requests))

	servers := make([]*grpc.Server, 0, len(requests))
	for i, lis := range listeners {
		server := newServer(requests[i].handler)
		go runServer(&wg, serverErrsChan, lis, server)
		servers = append(servers, server)
	}

	go func() {
		wg.Wait()
		close(serverErrsChan)
	}()

	return serverBundle{servers: servers, serverErrsChan: serverErrsChan}, nil
}

type serverConfig struct {
	port    int
	handler *client.Handler
}

type serverBundle struct {
	servers        []*grpc.Server
	serverErrsChan <-chan error
}

func newServer(handler *client.Handler) *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterClientServiceServer(server, handler)
	return server
}

func runServer(wg *sync.WaitGroup, errCh chan<- error, lis net.Listener, srv *grpc.Server) {
	defer wg.Done()
	if err := srv.Serve(lis); err != nil {
		errCh <- err
	}
}
