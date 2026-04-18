package client

import "context"

type operation func(transactionID string) error

type transactionPreparer interface {
	prepareTransaction(ctx context.Context, transactionID string, payload string) (bool, error)
}

type transactionCommiter interface {
	commitTransaction(ctx context.Context, transactionID string) (bool, error)
}

type transactionRollbacker interface {
	rollbackTransaction(ctx context.Context, transactionID string) (bool, error)
}
