package test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type distributedTransaction struct {
	transactionID string
	transactions  []transaction
}

type transaction struct {
	participantNumber int
	payload           string
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
	serverConfigs          []serverConfig
	txCoordinator          *twopc.Coordinator[string]
	distributedTransaction distributedTransaction
	wantErr                bool
	wantedOutcome          twopc.Outcome
}

func runTest(t *testing.T, tt testCase) {
	t.Helper()

	srvBundle, err := runServers(tt.serverConfigs)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addresses := srvBundle.addresses()
	outcome := tt.txCoordinator.Execute(ctx, tt.distributedTransaction.toTwopc(addresses))
	if tt.wantedOutcome != outcome.Outcome() {
		t.Fatalf("expected outcome %v, got %v", tt.wantedOutcome, outcome.Outcome())
	}
	if tt.wantErr && outcome.Err() == nil {
		t.Fatalf("expected error")
	}
	if !tt.wantErr && outcome.Err() != nil {
		t.Fatalf("didn't expect error, got %v", outcome.Err())
	}

	errs := shutdownServerBundle(srvBundle)
	if len(errs) != 0 {
		t.Errorf("got %d server errors: %v", len(errs), errs)
	}
}

func shutdownServerBundle(srvBundle serverBundle) []error {
	for _, server := range srvBundle.servers {
		go server.grpcServer.GracefulStop()
	}
	var errs []error
	for err := range srvBundle.serverErrsChan {
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}
