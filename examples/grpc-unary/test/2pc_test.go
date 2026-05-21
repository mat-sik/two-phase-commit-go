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
	tests := []struct {
		name          string
		serverConfigs []serverConfig
		txCoordinator *twopc.Coordinator[string]
		request       twopc.DistributedTransaction[string]
		wantErr       bool
		wantedOutcome twopc.Outcome
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
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewGRPCClient,
			),
			request: twopc.DistributedTransaction[string]{
				TransactionID: "tx-1",
				Transactions: []twopc.Transaction[string]{
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30050),
						Payload:       "one",
					},
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30051),
						Payload:       "two",
					},
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30052),
						Payload:       "three",
					},
				},
			},
			wantErr:       false,
			wantedOutcome: twopc.OutcomeCommitted,
		},
		{
			name: "Some failing on prepare some other on rollback, but eventually all rollbacks go through",
			serverConfigs: []serverConfig{
				{
					port:    30050,
					handler: client.NewFailingNoopHandler(1, 0, 1),
				},
				{
					port:    30051,
					handler: client.NewFailingNoopHandler(0, 0, 1),
				},
				{
					port:    30052,
					handler: client.NewFailingNoopHandler(1, 0, 0),
				},
			},
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewGRPCClient,
			),
			request: twopc.DistributedTransaction[string]{
				TransactionID: "tx-1",
				Transactions: []twopc.Transaction[string]{
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30050),
						Payload:       "one",
					},
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30051),
						Payload:       "two",
					},
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30052),
						Payload:       "three",
					},
				},
			},
			wantErr:       true,
			wantedOutcome: twopc.OutcomeRolledBack,
		},
		{
			name: "some commits fail, but eventually all commits go through",
			serverConfigs: []serverConfig{
				{
					port:    30050,
					handler: client.NewFailingNoopHandler(0, 1, 0),
				},
				{
					port:    30051,
					handler: client.NewFailingNoopHandler(0, 1, 0),
				},
				{
					port:    30052,
					handler: client.NewFailingNoopHandler(0, 1, 0),
				},
			},
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewGRPCClient,
			),
			request: twopc.DistributedTransaction[string]{
				TransactionID: "tx-1",
				Transactions: []twopc.Transaction[string]{
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30050),
						Payload:       "one",
					},
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30051),
						Payload:       "two",
					},
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30052),
						Payload:       "three",
					},
				},
			},
			wantErr:       true,
			wantedOutcome: twopc.OutcomeCommitted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			srvBundle, err := runServers(tt.serverConfigs)
			if err != nil {
				t.Fatalf("failed to listen: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			outcome := tt.txCoordinator.Execute(ctx, tt.request)
			if tt.wantedOutcome != outcome.Outcome() {
				t.Fatalf("expected outcome %v, got %v", tt.wantedOutcome, outcome.Outcome())
			}
			if tt.wantErr && outcome.Err() == nil {
				t.Fatalf("expected error")
			}
			if !tt.wantErr && outcome.Err() != nil {
				t.Fatalf("didn't expect error, got %v", outcome.Err())
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
