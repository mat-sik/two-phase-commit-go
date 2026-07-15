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
	persistenceConfig    twopc.PersistenceConfig[string]
	clientConfigProvider clientConfigProvider
	opts                 []twopc.Option
}

type clientConfigProvider func(clientTypeByParticipantID map[string]clientType) twopc.ClientConfig[string]

func newClientConfig(clientConfig twopc.ClientConfig[string]) clientConfigProvider {
	return constant[map[string]clientType, twopc.ClientConfig[string]](clientConfig)
}

func constant[In, Out any](output Out) func(In) Out {
	return func(_ In) Out {
		return output
	}
}

func newMixedClientConfig() clientConfigProvider {
	return func(clientTypeByParticipantID map[string]clientType) twopc.ClientConfig[string] {
		return twopc.ClientConfig[string]{
			NewClientFunc: func(participantID string) (twopc.Client, error) {
				participantClientType := clientTypeByParticipantID[participantID]
				return clientFor(participantClientType)(participantID)
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
	protocol          clientType
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

	participantTransports := newParticipantTransports(tt.distributedTransaction.transactions)

	txCoordinator := newCoordinator(
		tt.coordinatorConfig.persistenceConfig,
		tt.coordinatorConfig.clientConfigProvider,
		participantTransports,
		addresses,
		tt.coordinatorConfig.opts...,
	)

	outcome := txCoordinator.Execute(ctx, tt.distributedTransaction.toTwopc(addresses))

	assertOutcome(t, tt.wantErr, tt.wantedOutcome, outcome)

	if errs := srvBundle.shutdown(); len(errs) > 0 {
		t.Errorf("got %d server errors: %v", len(errs), errs)
	}
}

func newParticipantTransports(transactions []transaction) map[int]clientType {
	participantTransports := make(map[int]clientType, len(transactions))
	for _, tx := range transactions {
		participantTransports[tx.participantNumber] = tx.protocol
	}
	return participantTransports
}

func newCoordinator(
	persistenceConfig twopc.PersistenceConfig[string],
	clientConfigProvider clientConfigProvider,
	participantClientTypes map[int]clientType,
	addresses []string,
	opts ...twopc.Option,
) *twopc.Coordinator[string] {
	clientTypeByParticipantId := make(map[string]clientType, len(participantClientTypes))
	for participantNumber, participantClientType := range participantClientTypes {
		clientTypeByParticipantId[addresses[participantNumber]] = participantClientType
	}

	txCoordinatorClientConfig := clientConfigProvider(clientTypeByParticipantId)
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

type clientType int

const (
	clientTypeREST         clientType = iota
	clientTypeBasicGRPC    clientType = iota
	clientTypeTransferGRPC clientType = iota
)

func clientFor(t clientType) func(participantID string) (twopc.Client, error) {
	switch t {
	case clientTypeREST:
		return client.NewRESTClient
	case clientTypeBasicGRPC:
		return basic.NewGRPCClient
	case clientTypeTransferGRPC:
		return transfer.NewGRPCClient
	default:
		panic(fmt.Sprintf("unsupported transport type: %d", t))
	}
}
