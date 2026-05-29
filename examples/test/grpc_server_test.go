package test

import (
	"net"
	"sync"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	pb "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/client/v1"
	"google.golang.org/grpc"
)

func runServers(requests []serverConfig) (serverBundle, error) {
	listeners := make([]net.Listener, 0, len(requests))
	for range requests {
		lis, err := net.Listen("tcp", ":0")
		if err != nil {
			return serverBundle{}, err
		}
		listeners = append(listeners, lis)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(requests))

	serverErrsChan := make(chan error, len(requests))

	servers := make([]testServer, 0, len(requests))
	for i, lis := range listeners {
		server := newServer(requests[i].handler)
		go runServer(&wg, serverErrsChan, lis, server)

		randomAddress := lis.Addr().String()

		servers = append(servers, testServer{
			grpcServer: server,
			address:    randomAddress,
		})
	}

	go func() {
		wg.Wait()
		close(serverErrsChan)
	}()

	return serverBundle{servers: servers, serverErrsChan: serverErrsChan}, nil
}

type serverConfig struct {
	handler *client.GRPCHandler
}

type serverBundle struct {
	servers        []testServer
	serverErrsChan <-chan error
}

func (sb serverBundle) addresses() []string {
	addresses := make([]string, 0, len(sb.servers))
	for _, srv := range sb.servers {
		addresses = append(addresses, srv.address)
	}
	return addresses
}

type testServer struct {
	grpcServer *grpc.Server
	address    string
}

func newServer(handler *client.GRPCHandler) *grpc.Server {
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
