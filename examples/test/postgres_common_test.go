//go:build testcontainers

package test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type testContainersTestCase[T any] struct {
	name                   string
	handlers               []T
	handlersProviders      []handlerProvider[T]
	handlersMapper         func([]T) []client.RunServerRequest
	txCoordinatorProvider  txCoordinatorProvider
	distributedTransaction distributedTransaction
	wantErr                bool
	wantedOutcome          twopc.Outcome
}

func runTestContainersTest[T any](t *testing.T, tt testContainersTestCase[T]) {
	t.Helper()

	coordinatorPool, participantPools := runPostgresForPools(t, len(tt.handlersProviders))

	handlers := getHandlers(tt, participantPools)

	srvBundle, err := client.RunServers(tt.handlersMapper(handlers))
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addresses := srvBundle.Addresses()
	txCoordinator := tt.txCoordinatorProvider(coordinatorPool)
	outcome := txCoordinator.Execute(ctx, tt.distributedTransaction.toTwopc(addresses))

	assertOutcome(t, tt.wantErr, tt.wantedOutcome, outcome)

	errs := srvBundle.Shutdown()
	if len(errs) > 0 {
		t.Errorf("got %d server errors: %v", len(errs), errs)
	}
}

func runPostgresForPools(t *testing.T, participantAmount int) (*pgxpool.Pool, []*pgxpool.Pool) {
	t.Helper()

	coordinatorCh := make(chan runContainerResult, participantAmount)
	go func() {
		pool, terminator, err := runPostgresForCoordinatorPool(t.Context())
		coordinatorCh <- runContainerResult{
			pool:       pool,
			terminator: terminator,
			err:        err,
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(participantAmount)
	participantCh := make(chan runContainerResult, participantAmount)
	for range participantAmount {
		go func() {
			defer wg.Done()
			pool, terminator, err := runPostgresForParticipantPool(t.Context())
			participantCh <- runContainerResult{
				pool:       pool,
				terminator: terminator,
				err:        err,
			}
		}()
	}
	go func() {
		wg.Wait()
		close(participantCh)
	}()

	var errs []error

	var coordinatorPool *pgxpool.Pool
	if coordinatorResult := <-coordinatorCh; coordinatorResult.err != nil {
		errs = append(errs, coordinatorResult.err)
	} else {
		coordinatorPool = coordinatorResult.pool
		t.Cleanup(coordinatorResult.terminator)
	}

	var participantPools []*pgxpool.Pool
	for participantResult := range participantCh {
		if participantResult.err != nil {
			errs = append(errs, participantResult.err)
			continue
		}
		participantPools = append(participantPools, participantResult.pool)
		t.Cleanup(participantResult.terminator)
	}

	if len(errs) > 0 {
		t.Fatalf("failed to run required amount of postgres containers: %v", errors.Join(errs...))
	}

	return coordinatorPool, participantPools
}

type runContainerResult struct {
	pool       *pgxpool.Pool
	terminator postgresTerminator
	err        error
}

func getHandlers[T any](tt testContainersTestCase[T], participantPools []*pgxpool.Pool) []T {
	if tt.handlers != nil {
		return tt.handlers
	}

	handlers := make([]T, 0, len(tt.handlersProviders))
	for i, provider := range tt.handlersProviders {
		handlers = append(handlers, provider(participantPools[i]))
	}
	return handlers
}

type txCoordinatorProvider func(pool *pgxpool.Pool) *twopc.Coordinator[string]

type handlerProvider[T any] func(pool *pgxpool.Pool) T
