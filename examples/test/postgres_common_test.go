//go:build testcontainers

package test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"strconv"
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

	coordinatorPool, coordinatorDbDropper := createCoordinatorDb(t.Context(), t.Name())
	t.Cleanup(coordinatorDbDropper)

	handlers, clientDBDroppers := getHandlers(t.Context(), tt)
	for _, dbDropper := range clientDBDroppers {
		t.Cleanup(dbDropper)
	}

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

func getHandlers[T any](ctx context.Context, tt testContainersTestCase[T]) ([]T, []databaseDropper) {
	if tt.handlers != nil {
		return tt.handlers, nil
	}

	handlers := make([]T, 0, len(tt.handlersProviders))
	dbDroppers := make([]databaseDropper, 0, len(tt.handlersProviders))
	for _, provider := range tt.handlersProviders {
		clientPool, dbDropper := createClientDb(ctx, provider.getPort())
		handlers = append(handlers, provider.providerFunc(clientPool))
		dbDroppers = append(dbDroppers, dbDropper)
	}
	return handlers, dbDroppers
}

func assertOutcome(t *testing.T, wantErr bool, wantedOutcome twopc.Outcome, result twopc.Result) {
	t.Helper()
	if wantErr && result.Err() == nil {
		t.Fatalf("expected error")
	}
	if !wantErr && result.Err() != nil {
		t.Fatalf("didn't expect error, got %v", result.Err())
	}
	if wantedOutcome != result.Outcome() {
		t.Fatalf("expected outcome %v, got %v", wantedOutcome, result.Outcome())
	}
}

func createClientDb(ctx context.Context, port int) (*pgxpool.Pool, databaseDropper) {
	return createDatabaseFunc(ctx, strconv.Itoa(port), "testdata/client-schema.sql")
}

func createCoordinatorDb(ctx context.Context, testName string) (*pgxpool.Pool, databaseDropper) {
	return createDatabaseFunc(ctx, uniqueCoordinatorDbName(testName), "testdata/coordinator-schema.sql")
}

func uniqueCoordinatorDbName(testName string) string {
	return fmt.Sprintf("coordinator_%s", getTestHashID(testName))
}

func getTestHashID(testName string) string {
	hash := sha256.Sum256([]byte(testName))
	return hex.EncodeToString(hash[:])
}

type txCoordinatorProvider func(pool *pgxpool.Pool) *twopc.Coordinator[string]

type handlerProvider[T any] struct {
	providerFunc func(pool *pgxpool.Pool) T
	port         *int
}

func (hp handlerProvider[T]) getPort() int {
	if hp.port != nil {
		return *hp.port
	}

	const minPort = 32768
	const maxPort = 60999
	const rangeSize = maxPort - minPort + 1
	return minPort + rand.N(rangeSize)
}
