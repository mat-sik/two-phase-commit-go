package twopc

import (
	"context"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/client"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

type TransactionStateChecker[ID comparable] interface {
	Check(transactionID string) map[ID]TransactionState
}

type TransactionState int

const (
	NotStarted TransactionState = iota
	Prepared
	PrepareFailed
	Committed
	RolledBack
)

func (ts TransactionState) toInternal() transaction.State {
	switch ts {
	case NotStarted:
		return transaction.NotStarted
	case Prepared:
		return transaction.Prepared
	case PrepareFailed:
		return transaction.PrepareFailed
	case Committed:
		return transaction.Committed
	case RolledBack:
		return transaction.RolledBack
	default:
		panic("unsupported TransactionState")
	}
}

type internalTransactionStateCheckerAdapter[ID comparable] struct {
	transactionStateChecker TransactionStateChecker[ID]
}

func (sc internalTransactionStateCheckerAdapter[ID]) Check(transactionID string) map[ID]transaction.State {
	transactionStates := sc.transactionStateChecker.Check(transactionID)
	mappedToInternal := make(map[ID]transaction.State, len(transactionStates))
	for k, v := range transactionStates {
		mappedToInternal[k] = v.toInternal()
	}
	return mappedToInternal
}

type StatePersister[ID comparable] interface {
	PersistState(ctx context.Context, transactionID string, clientID ID, transactionState TransactionState) <-chan PersistResult
}

type statePersister[ID comparable] interface {
	PersistState(ctx context.Context, transactionID string, clientID ID, transactionState transaction.State) <-chan PersistResult
}

type internalStatePersisterAdapter[ID comparable] struct {
	statePersister StatePersister[ID]
}

func (i internalStatePersisterAdapter[ID]) PersistState(ctx context.Context, transactionID string, clientID ID, transactionState transaction.State) <-chan PersistResult {
	return i.statePersister.PersistState(ctx, transactionID, clientID, toExposed(transactionState))
}

func toExposed(transactionState transaction.State) TransactionState {
	switch transactionState {
	case transaction.NotStarted:
		return NotStarted
	case transaction.Prepared:
		return Prepared
	case transaction.PrepareFailed:
		return PrepareFailed
	case transaction.Committed:
		return Committed
	case transaction.RolledBack:
		return RolledBack
	default:
		panic("unsupported transaction.State")
	}
}

type PersistResult struct {
	Commit   func() error
	Rollback func() error
	Err      error
}

type PreparePayload interface{}

type Client interface {
	PrepareTransaction(ctx context.Context, transactionID string, payload PreparePayload) error
	CommitTransaction(ctx context.Context, transactionID string) error
	RollbackTransaction(ctx context.Context, transactionID string) error
}

type internalClientAdapter struct {
	client Client
}

func (c internalClientAdapter) PrepareTransaction(ctx context.Context, transactionID string, payload client.PreparePayload) error {
	return c.client.PrepareTransaction(ctx, transactionID, payload)
}

func (c internalClientAdapter) CommitTransaction(ctx context.Context, transactionID string) error {
	return c.client.CommitTransaction(ctx, transactionID)
}

func (c internalClientAdapter) RollbackTransaction(ctx context.Context, transactionID string) error {
	return c.client.RollbackTransaction(ctx, transactionID)
}

func adaptForInternalNewClientFunc[ID comparable](newClientFunc func(clientID ID) (Client, error)) func(clientID ID) (client.Client, error) {
	return func(clientID ID) (client.Client, error) {
		externalClient, err := newClientFunc(clientID)
		if err != nil {
			return internalClientAdapter{}, err
		}
		return internalClientAdapter{client: externalClient}, nil
	}
}
