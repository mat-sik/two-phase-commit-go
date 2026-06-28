package test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/basic"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/transfer"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type testCase struct {
	name                   string
	runServerRequests      []runServerRequest
	coordinatorConfig      coordinatorConfig
	distributedTransaction distributedTransaction
	wantErr                bool
	wantedOutcome          twopc.Outcome
}

type coordinatorConfig struct {
	persistenceConfig twopc.PersistenceConfig[string]
	clientConfig      coordinatorClientConfig
	opts              []twopc.Option
}

type coordinatorClientConfig struct {
	clientConfigProvider  clientConfigProvider
	participantTransports map[int]transportType
}

type clientConfigProvider func(transportTypeByParticipantID map[string]transportType) twopc.ClientConfig[string]

func newClientConfig(clientConfig twopc.ClientConfig[string]) clientConfigProvider {
	return constant[map[string]transportType, twopc.ClientConfig[string]](clientConfig)
}

func constant[In, Out any](output Out) func(In) Out {
	return func(_ In) Out {
		return output
	}
}

func newMixedClientConfig() clientConfigProvider {
	return func(transportTypeByParticipantID map[string]transportType) twopc.ClientConfig[string] {
		return twopc.ClientConfig[string]{
			NewClientFunc: func(participantID string) (twopc.Client, error) {
				participantTransportType := transportTypeByParticipantID[participantID]
				return clientFor(participantTransportType)(participantID)
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

	persistenceConfig := tt.coordinatorConfig.persistenceConfig
	clientConfig := tt.coordinatorConfig.clientConfig
	coordinatorOpts := tt.coordinatorConfig.opts
	txCoordinator := newCoordinator(persistenceConfig, clientConfig, addresses, coordinatorOpts...)

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
	transportTypeByParticipantId := make(map[string]transportType, len(clientConfig.participantTransports))
	for participantNumber, participantTransportType := range clientConfig.participantTransports {
		transportTypeByParticipantId[addresses[participantNumber]] = participantTransportType
	}

	txCoordinatorClientConfig := clientConfig.clientConfigProvider(transportTypeByParticipantId)
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
	if !wantErr && result.Err() != nil && !errors.Is(result.Err(), context.Canceled) {
		t.Fatalf("didn't expect error, got %v", result.Err())
	}
	if wantedOutcome != result.Outcome() {
		t.Fatalf("expected outcome %v, got %v", wantedOutcome, result.Outcome())
	}
}

type transportType int

const (
	transportTypeREST         transportType = iota
	transportTypeBasicGRPC    transportType = iota
	transportTypeTransferGRPC transportType = iota
)

func clientFor(transportType transportType) func(participantID string) (twopc.Client, error) {
	switch transportType {
	case transportTypeREST:
		return client.NewRESTClient
	case transportTypeBasicGRPC:
		return basic.NewGRPCClient
	case transportTypeTransferGRPC:
		return transfer.NewGRPCClient
	default:
		panic(fmt.Sprintf("unsupported transport type: %d", transportType))
	}
}
