//go:build testcontainers

package test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type testContainersTestCase struct {
	name                   string
	runServerRequests      []client.RunServerRequest
	txCoordinatorProvider  txCoordinatorProvider
	distributedTransaction distributedTransaction
	wantErr                bool
	wantedOutcome          twopc.Outcome
}

func runTestContainersTest(t *testing.T, tt testContainersTestCase) {
	t.Helper()

	coordinatorPool, coordinatorDbDropper := createCoordinatorDb(t)
	t.Cleanup(coordinatorDbDropper)

	srvBundle, err := client.RunServers(tt.runServerRequests)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addresses := srvBundle.Addresses()
	txCoordinator := tt.txCoordinatorProvider(coordinatorPool)
	outcome := txCoordinator.Execute(ctx, tt.distributedTransaction.toTwopc(t, addresses))

	assertOutcome(t, tt.wantErr, tt.wantedOutcome, outcome)

	errs := srvBundle.Shutdown()
	if len(errs) > 0 {
		t.Errorf("got %d server errors: %v", len(errs), errs)
	}
}

func assertOutcome(t *testing.T, wantErr bool, wantedOutcome twopc.Outcome, result twopc.Result) {
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

func createCoordinatorDb(t *testing.T) (*pgxpool.Pool, databaseDropper) {
	t.Helper()
	return createDatabaseFunc(t.Context(), uniqueCoordinatorDbName(t), "testdata/coordinator-schema.sql")
}

func uniqueCoordinatorDbName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("coordinator_%s", getTestHashID(t))
}

func getTestHashID(t *testing.T) string {
	t.Helper()

	hash := sha256.Sum256([]byte(t.Name()))

	return hex.EncodeToString(hash[:])
}

type txCoordinatorProvider func(pool *pgxpool.Pool) *twopc.Coordinator[string]
