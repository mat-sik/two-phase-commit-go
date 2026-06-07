package test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type testCase struct {
	name                   string
	runServerRequests      []runServerRequest
	txCoordinator          *twopc.Coordinator[string]
	distributedTransaction distributedTransaction
	wantErr                bool
	wantedOutcome          twopc.Outcome
}

func runTest(t *testing.T, tt testCase) {
	t.Helper()

	srvBundle, err := runServers(tt.runServerRequests)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addresses := srvBundle.addresses()
	outcome := tt.txCoordinator.Execute(ctx, tt.distributedTransaction.toTwopc(addresses))

	assertOutcome(t, tt.wantErr, tt.wantedOutcome, outcome)

	errs := srvBundle.shutdown()
	if len(errs) > 0 {
		t.Errorf("got %d server errors: %v", len(errs), errs)
	}
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

type distributedTransaction struct {
	transactionID string
	transactions  []transaction
}

type transaction struct {
	participantNumber int
	payload           twopc.PreparePayload
}

func (dt distributedTransaction) toTwopc(addresses []string) twopc.DistributedTransaction[string] {
	transactions := make([]twopc.Transaction[string], 0, len(dt.transactions))
	for _, tx := range dt.transactions {
		transactions = append(transactions, tx.toTwopc(addresses))
	}
	return twopc.DistributedTransaction[string]{
		TransactionID: dt.transactionID,
		Transactions:  transactions,
	}
}

func (tx transaction) toTwopc(addresses []string) twopc.Transaction[string] {
	participantID := fmt.Sprintf("localhost:%d", rand.Intn(65535-1024)+1024)
	if tx.participantNumber <= len(addresses)-1 {
		participantID = addresses[tx.participantNumber]
	}
	return twopc.Transaction[string]{
		ParticipantID: participantID,
		Payload:       tx.payload,
	}
}
