package test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/examples/grpc-unary/internal/client"
	"github.com/mat-sik/two-phase-commit-go/examples/grpc-unary/internal/coordinator"
	pb "github.com/mat-sik/two-phase-commit-go/examples/grpc-unary/internal/generated/client/v1"
	"github.com/mat-sik/two-phase-commit-go/twopc"
	"google.golang.org/grpc"
)

func Test_integration(t *testing.T) {
	t.Run("foo", func(t *testing.T) {
		firstClientPort := 30050
		secondClientPort := 30051
		thirdClientPort := 30052

		srvBundle, err := runServers([]serverConfig{
			{
				port:    firstClientPort,
				handler: client.NewNoopHandler(),
			},
			{
				port:    secondClientPort,
				handler: client.NewNoopHandler(),
			},
			{
				port:    thirdClientPort,
				handler: client.NewNoopHandler(),
			},
		})
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}

		coord := twopc.NewCoordinator(
			coordinator.MockTransactionStateChecker{},
			coordinator.MockStatePersister{},
			coordinator.NewGRPCClient,
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		req := twopc.DistributedTransaction[string]{
			TransactionID: "tx-1",
			Transactions: []twopc.Transaction[string]{
				{
					ClientID: fmt.Sprintf("localhost:%d", firstClientPort),
					Payload:  "one",
				},
				{
					ClientID: fmt.Sprintf("localhost:%d", secondClientPort),
					Payload:  "two",
				},
				{
					ClientID: fmt.Sprintf("localhost:%d", thirdClientPort),
					Payload:  "three",
				},
			},
		}

		if err = coord.Execute(ctx, req); err != nil {
			t.Fatal(err)
		}

		for _, server := range srvBundle.servers {
			go func() {
				server.GracefulStop()
			}()
		}

		var errs []error
		for err = range srvBundle.serverErrsChan {
			if err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) != 0 {
			t.Errorf("got %d errors: %v", len(errs), errs)
		}
	})
}

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
