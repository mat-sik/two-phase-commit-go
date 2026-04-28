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

		operationHandler := coordinator.NewOperationHandler(
			coordinator.NewStateLoader(mockTransactionStateChecker{}),
			mockStatePersister{},
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		req := coordinator.AtomicTransactions{
			TransactionID: "tx-1",
			Transactions: []coordinator.Transaction{
				{TargetHost: fmt.Sprintf("localhost:%d", firstClientPort), Payload: "one"},
				{TargetHost: fmt.Sprintf("localhost:%d", secondClientPort), Payload: "two"},
				{TargetHost: fmt.Sprintf("localhost:%d", thirdClientPort), Payload: "three"},
			},
		}

		if err = operationHandler.HandleRequest(ctx, req); err != nil {
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

func (m mockStatePersister) PersistState(_ context.Context, _ string, _ string, _ coordinator.TransactionState) <-chan coordinator.PersistResult {
	ch := make(chan coordinator.PersistResult, 1)
	if m.err != nil {
		ch <- coordinator.PersistResult{Err: m.err}
	} else {
		ch <- coordinator.PersistResult{
			Commit:   func() error { return nil },
			Rollback: func() error { return nil },
		}
	}
	return ch
}

type mockTransactionStateChecker struct {
	stateByHost map[string]coordinator.TransactionState
}

func (m mockTransactionStateChecker) Check(_ string) map[string]coordinator.TransactionState {
	return m.stateByHost
}
