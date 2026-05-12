package test

import (
	"context"
	"errors"
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
	tests := []struct {
		name          string
		serverConfigs []serverConfig
		coord         *twopc.Coordinator[string]
		request       twopc.DistributedTransaction[string]
		wantedErr     error
	}{
		{
			name: "Simple happy path",
			serverConfigs: []serverConfig{
				{
					port:    30050,
					handler: client.NewNoopHandler(),
				},
				{
					port:    30051,
					handler: client.NewNoopHandler(),
				},
				{
					port:    30052,
					handler: client.NewNoopHandler(),
				},
			},
			coord: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewGRPCClient,
			),
			request: twopc.DistributedTransaction[string]{
				TransactionID: "tx-1",
				Transactions: []twopc.Transaction[string]{
					{
						ClientID: fmt.Sprintf("localhost:%d", 30050),
						Payload:  "one",
					},
					{
						ClientID: fmt.Sprintf("localhost:%d", 30051),
						Payload:  "two",
					},
					{
						ClientID: fmt.Sprintf("localhost:%d", 30052),
						Payload:  "three",
					},
				},
			},
			wantedErr: nil,
		},
		{
			name: "One Failing client on prepare",
			serverConfigs: []serverConfig{
				{
					port:    30050,
					handler: client.NewFailingNoopHandler(3, 0, 0),
				},
				{
					port:    30051,
					handler: client.NewNoopHandler(),
				},
				{
					port:    30052,
					handler: client.NewNoopHandler(),
				},
			},
			coord: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewGRPCClient,
			),
			request: twopc.DistributedTransaction[string]{
				TransactionID: "tx-1",
				Transactions: []twopc.Transaction[string]{
					{
						ClientID: fmt.Sprintf("localhost:%d", 30050),
						Payload:  "one",
					},
					{
						ClientID: fmt.Sprintf("localhost:%d", 30051),
						Payload:  "two",
					},
					{
						ClientID: fmt.Sprintf("localhost:%d", 30052),
						Payload:  "three",
					},
				},
			},
			wantedErr: twopc.ErrRollback,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			srvBundle, err := runServers(tt.serverConfigs)
			if err != nil {
				t.Fatalf("failed to listen: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			err = tt.coord.Execute(ctx, tt.request)

			if err != nil {
				if tt.wantedErr == nil {
					t.Fatal(err)
				}
				if !errors.Is(err, tt.wantedErr) {
					t.Fatalf("expected error %v, got %v", tt.wantedErr, err)
				}
			} else if tt.wantedErr != nil {
				t.Fatalf("expected error %v, but got no err", tt.wantedErr)
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
