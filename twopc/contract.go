package twopc

import (
	"context"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/client"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

type TransactionStateChecker[ID comparable] interface {
	Check(transactionID string) map[ID]transaction.State
}

type StatePersister[ID comparable] interface {
	PersistState(ctx context.Context, transactionID string, clientID ID, transactionState transaction.State) <-chan PersistResult
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
