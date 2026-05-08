package test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/grpc-unary/internal/client"
	"github.com/mat-sik/two-phase-commit-go/grpc-unary/internal/coordinator"
	pb "github.com/mat-sik/two-phase-commit-go/grpc-unary/internal/generated/client/v1"
	"github.com/mat-sik/two-phase-commit-go/twopc"
	"google.golang.org/grpc"
)

func Test_integration(t *testing.T) {
	t.Run("foo", func(t *testing.T) {
		firstClientPort := 30050
		firstListener, err := net.Listen("tcp", fmt.Sprintf(":%d", firstClientPort))
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}
		secondClientPort := 30051
		secondListener, err := net.Listen("tcp", fmt.Sprintf(":%d", secondClientPort))
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}
		thirdClientPort := 30052
		thirdListener, err := net.Listen("tcp", fmt.Sprintf(":%d", thirdClientPort))
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}

		wg := sync.WaitGroup{}
		wg.Add(3)

		serverErrsChan := make(chan error, 3)

		firstServer := newServer(client.NewNoopHandler())
		go runServer(&wg, serverErrsChan, firstListener, firstServer)

		secondServer := newServer(client.NewNoopHandler())
		go runServer(&wg, serverErrsChan, secondListener, secondServer)

		thirdServer := newServer(client.NewNoopHandler())
		go runServer(&wg, serverErrsChan, thirdListener, thirdServer)

		go func() {
			wg.Wait()
			close(serverErrsChan)
		}()

		coord := twopc.NewCoordinator(
			mockTransactionStateChecker{},
			mockStatePersister{},
			coordinator.NewGRPCClient,
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		req := twopc.DistributedTransaction[string]{
			TransactionID: "tx-1",
			Transactions: []twopc.Transaction[string]{
				twopc.NewTransaction(fmt.Sprintf("localhost:%d", firstClientPort), "one"),
				twopc.NewTransaction(fmt.Sprintf("localhost:%d", secondClientPort), "two"),
				twopc.NewTransaction(fmt.Sprintf("localhost:%d", thirdClientPort), "three"),
			},
		}

		if err = coord.Execute(ctx, req); err != nil {
			t.Fatal(err)
		}

		go func() {
			firstServer.GracefulStop()
		}()
		go func() {
			secondServer.GracefulStop()
		}()
		go func() {
			thirdServer.GracefulStop()
		}()

		var errs []error
		for err = range serverErrsChan {
			if err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) != 0 {
			t.Errorf("got %d errors: %v", len(errs), errs)
		}
	})
}

func runServer(wg *sync.WaitGroup, errCh chan<- error, lis net.Listener, srv *grpc.Server) {
	defer wg.Done()
	if err := srv.Serve(lis); err != nil {
		errCh <- err
	}
}

func newServer(handler *client.Handler) *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterClientServiceServer(server, handler)
	return server
}

type mockStatePersister struct {
	err error
}

func (m mockStatePersister) PersistState(_ context.Context, _ string, _ string, _ twopc.TransactionState) <-chan twopc.PersistResult {
	ch := make(chan twopc.PersistResult, 1)
	if m.err != nil {
		ch <- twopc.PersistResult{Err: m.err}
	} else {
		ch <- twopc.PersistResult{
			Commit:   func() error { return nil },
			Rollback: func() error { return nil },
		}
	}
	return ch
}

type mockTransactionStateChecker struct {
	stateByClientID map[string]twopc.TransactionState
}

func (m mockTransactionStateChecker) Check(_ string) map[string]twopc.TransactionState {
	return m.stateByClientID
}
