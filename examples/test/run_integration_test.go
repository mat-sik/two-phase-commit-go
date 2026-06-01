package test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type distributedTransaction struct {
	transactionID string
	transactions  []transaction
}

type transaction struct {
	participantNumber int
	payload           twopc.PreparePayload
}

func (t transaction) toTwopc(addresses []string) twopc.Transaction[string] {
	participantID := fmt.Sprintf("localhost:%d", rand.Intn(65535-1024)+1024)
	if t.participantNumber <= len(addresses)-1 {
		participantID = addresses[t.participantNumber]
	}
	return twopc.Transaction[string]{
		ParticipantID: participantID,
		Payload:       t.payload,
	}
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

type testCase struct {
	name                   string
	runServerRequests      []client.RunServerRequest
	txCoordinator          *twopc.Coordinator[string]
	distributedTransaction distributedTransaction
	wantErr                bool
	wantedOutcome          twopc.Outcome
}

func runTest(t *testing.T, tt testCase) {
	t.Helper()

	srvBundle, err := client.RunServers(tt.runServerRequests)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addresses := srvBundle.Addresses()
	outcome := tt.txCoordinator.Execute(ctx, tt.distributedTransaction.toTwopc(addresses))
	if tt.wantErr && outcome.Err() == nil {
		t.Fatalf("expected error")
	}
	if !tt.wantErr && outcome.Err() != nil {
		t.Fatalf("didn't expect error, got %v", outcome.Err())
	}
	if tt.wantedOutcome != outcome.Outcome() {
		t.Fatalf("expected outcome %v, got %v", tt.wantedOutcome, outcome.Outcome())
	}

	errs := srvBundle.Shutdown()
	if len(errs) != 0 {
		t.Errorf("got %d server errors: %v", len(errs), errs)
	}
}
