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
	name                         string
	runServerRequests            []runServerRequest
	coordinatorPersistenceConfig twopc.PersistenceConfig[string]
	coordinatorClientConfig      coordinatorClientConfig
	coordinatorOpts              []twopc.Option
	distributedTransaction       distributedTransaction
	wantErr                      bool
	wantedOutcome                twopc.Outcome
}

type coordinatorClientConfig struct {
	clientConfigProvider   clientConfigProvider
	gRPCParticipantNumbers []int
}

type clientConfigProvider func(gRPCAddresses map[string]struct{}) twopc.ClientConfig[string]

func newClientConfig(clientConfig twopc.ClientConfig[string]) clientConfigProvider {
	return constant[map[string]struct{}, twopc.ClientConfig[string]](clientConfig)
}

func constant[In, Out any](output Out) func(In) Out {
	return func(_ In) Out {
		return output
	}
}

func newMixedClientConfig(
	gRPCNewClientFunc func(participantID string) (twopc.Client, error),
	restNewClientFunc func(participantID string) (twopc.Client, error),
) clientConfigProvider {
	return func(gRPCAddresses map[string]struct{}) twopc.ClientConfig[string] {
		return twopc.ClientConfig[string]{
			NewClientFunc: func(participantID string) (twopc.Client, error) {
				if _, ok := gRPCAddresses[participantID]; ok {
					return gRPCNewClientFunc(participantID)
				}
				return restNewClientFunc(participantID)
			},
		}
	}
}

type distributedTransaction struct {
	transactionID string
	transactions  []transaction
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

type transaction struct {
	participantNumber int
	payload           twopc.PreparePayload
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

func runTest(t *testing.T, tt testCase) {
	t.Helper()

	srvBundle, err := runServers(tt.runServerRequests)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addresses := srvBundle.addresses()

	txCoordinator := newCoordinator(tt.coordinatorPersistenceConfig, tt.coordinatorClientConfig, addresses, tt.coordinatorOpts...)

	outcome := txCoordinator.Execute(ctx, tt.distributedTransaction.toTwopc(addresses))

	assertOutcome(t, tt.wantErr, tt.wantedOutcome, outcome)

	if errs := srvBundle.shutdown(); len(errs) > 0 {
		t.Errorf("got %d server errors: %v", len(errs), errs)
	}
}

func newCoordinator(
	persistenceConfig twopc.PersistenceConfig[string],
	clientConfig coordinatorClientConfig,
	addresses []string,
	opts ...twopc.Option,
) *twopc.Coordinator[string] {
	grpcAddresses := make(map[string]struct{}, len(clientConfig.gRPCParticipantNumbers))
	for _, number := range clientConfig.gRPCParticipantNumbers {
		grpcAddresses[addresses[number]] = struct{}{}
	}

	txCoordinatorClientConfig := clientConfig.clientConfigProvider(grpcAddresses)
	return twopc.NewCoordinator(
		persistenceConfig,
		txCoordinatorClientConfig,
		opts...,
	)
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
