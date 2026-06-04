package client

import (
	"context"

	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type transactionPreparer interface {
	prepareTransaction(ctx context.Context, transactionID string, payload twopc.PreparePayload) error
}

type transactionCommiter interface {
	commitTransaction(ctx context.Context, transactionID string) error
}

type transactionRollbacker interface {
	rollbackTransaction(ctx context.Context, transactionID string) error
}
