package adapter

import (
	"context"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant"
)

type BasicTransactionPreparer interface {
	PrepareTransaction(ctx context.Context, transactionID string, payload string) error
}

type TransferTransactionPreparer interface {
	PrepareTransaction(ctx context.Context, transactionID string, payload participant.TransferPayload) error
}

type TransactionCommiter interface {
	CommitTransaction(ctx context.Context, transactionID string) error
}

type TransactionRollbacker interface {
	RollbackTransaction(ctx context.Context, transactionID string) error
}
