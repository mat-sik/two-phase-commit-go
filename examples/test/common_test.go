package test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/basic"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/transfer"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type testCase struct {
	name                   string
	serverSpecs            []serverSpec
	coordinatorConfig      coordinatorConfig
	distributedTransaction distributedTransaction
	wantErr                bool
	wantedOutcome          twopc.Outcome
}

type coordinatorConfig struct {
	persistenceConfig twopc.PersistenceConfig[string]
	opts              []twopc.Option
}

type distributedTransaction struct {
	transactionID string
	transactions  []transaction
}

func (dt distributedTransaction) toTwopc(addresses []string) twopc.DistributedTransaction[string] {
	transactions := make([]twopc.Transaction[string], 0, len(dt.transactions))
	for i := range len(dt.transactions) {
		transactions = append(transactions, dt.transactions[i].toTwopc(addresses[i]))
	}
	return twopc.DistributedTransaction[string]{
		TransactionID: dt.transactionID,
		Transactions:  transactions,
	}
}

type transaction struct {
	payload           twopc.PreparePayload
	communicationType communicationType
}

func (tx transaction) toTwopc(participantID string) twopc.Transaction[string] {
	return twopc.Transaction[string]{
		ParticipantID: participantID,
		Payload:       tx.payload,
	}
}

func runTest(t *testing.T, tt testCase) {
	t.Helper()

	srvBundle, err := runServers(toServerLaunches(tt.serverSpecs))
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}

	addresses := srvBundle.addresses()

	clientConfig := twopc.ClientConfig[string]{
		NewClientFunc: newClientFunc(tt.distributedTransaction.transactions, addresses),
	}

	txCoordinator := twopc.NewCoordinator(
		tt.coordinatorConfig.persistenceConfig,
		clientConfig,
		tt.coordinatorConfig.opts...,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	outcome := txCoordinator.Execute(ctx, tt.distributedTransaction.toTwopc(addresses))

	assertOutcome(t, tt.wantErr, tt.wantedOutcome, outcome)

	if errs := srvBundle.shutdown(); len(errs) > 0 {
		t.Errorf("got %d server errors: %v", len(errs), errs)
	}
}

func toServerLaunches(serverSpecs []serverSpec) []serverLaunch {
	requests := make([]serverLaunch, 0, len(serverSpecs))
	for _, spec := range serverSpecs {
		requests = append(requests, spec.toServerLaunch())
	}
	return requests
}

func newClientFunc(transactions []transaction, addresses []string) func(string) (twopc.Client, error) {
	newClientFuncByParticipantID := make(map[string]func(string) (twopc.Client, error))
	for i := range len(transactions) {
		newClientFuncByParticipantID[addresses[i]] = transactions[i].communicationType.clientFunc()
	}
	return func(participantID string) (twopc.Client, error) {
		return newClientFuncByParticipantID[participantID](participantID)
	}
}

func assertOutcome(t *testing.T, wantErr bool, wantedOutcome twopc.Outcome, result twopc.Result) {
	t.Helper()
	if wantErr && result.Err() == nil {
		t.Fatalf("expected error")
	}
	if !wantErr && result.Err() != nil && !errors.Is(result.Err(), context.Canceled) {
		t.Fatalf("didn't expect error, got %v", result.Err())
	}
	if wantedOutcome != result.Outcome() {
		t.Fatalf("expected outcome %v, got %v", wantedOutcome, result.Outcome())
	}
}

type communicationType int

const (
	communicationTypeRest = iota
	communicationTypeBasicGRPC
	communicationTypeTransferGrpc
)

func (ct communicationType) clientFunc() func(participantID string) (twopc.Client, error) {
	switch ct {
	case communicationTypeRest:
		return client.NewRESTClient
	case communicationTypeBasicGRPC:
		return basic.NewGRPCClient
	case communicationTypeTransferGrpc:
		return transfer.NewGRPCClient
	default:
		panic(fmt.Sprintf("unsupported communication type: %T", ct))
	}
}
