package twopc

import (
	"context"
	"fmt"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/participant"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

// TransactionStateChecker is used by the Coordinator to recover the persisted
// state of a distributed transaction, typically on startup after a crash.
// The returned map associates each participant ID with its last known state.
type TransactionStateChecker[ID comparable] interface {
	Check(ctx context.Context, transactionID string) (map[ID]TransactionState, error)
}

// TransactionState represents the lifecycle state of a single participant's
// transaction within the two-phase commit protocol.
type TransactionState int

const (
	// TransactionNotStarted means the participant has not yet been contacted.
	TransactionNotStarted TransactionState = iota
	// TransactionPrepared means the participant has successfully completed the prepare phase
	// and is ready to commit.
	TransactionPrepared
	// TransactionPrepareFailed means the participant responded with a failure during the
	// prepare phase, triggering a global rollback.
	TransactionPrepareFailed
	// TransactionCommitted means the participant has durably committed the transaction.
	TransactionCommitted
	// TransactionRolledBack means the participant has rolled back the transaction.
	TransactionRolledBack
)

func (ts TransactionState) toInternal() transaction.State {
	switch ts {
	case TransactionNotStarted:
		return transaction.NotStarted
	case TransactionPrepared:
		return transaction.Prepared
	case TransactionPrepareFailed:
		return transaction.PrepareFailed
	case TransactionCommitted:
		return transaction.Committed
	case TransactionRolledBack:
		return transaction.RolledBack
	default:
		panic("unsupported TransactionState")
	}
}

type internalTransactionStateCheckerAdapter[ID comparable] struct {
	transactionStateChecker TransactionStateChecker[ID]
}

func (a internalTransactionStateCheckerAdapter[ID]) Check(ctx context.Context, txID string) (map[ID]transaction.State, error) {
	transactionStates, err := a.transactionStateChecker.Check(ctx, txID)
	if err != nil {
		return nil, fmt.Errorf("checking tx %s states: %w", txID, err)
	}

	mappedToInternal := make(map[ID]transaction.State, len(transactionStates))
	for k, v := range transactionStates {
		mappedToInternal[k] = v.toInternal()
	}

	return mappedToInternal, nil
}

// TransactionStatePersister durably records a participant's state transition before the
// coordinator considers the transition final.
//
// Implementations should write the state change to a durable store (e.g. a
// database) and return a PersistResult over the channel. The coordinator will
// call Commit on the result if the network operation to the participant also
// succeeded, or Rollback if it did not — allowing the persisted record to
// stay consistent with what was actually sent.
//
// PersistState must not block; it should start the work asynchronously and
// return the channel immediately.
type TransactionStatePersister[ID comparable] interface {
	// PersistState TODO: simplify this interface to be synchronous, no need to complicate the life for the user
	PersistState(ctx context.Context, transactionID string, participantID ID, transactionState TransactionState) <-chan PersistResult
}

type transactionStatePersister[ID comparable] interface {
	PersistState(ctx context.Context, transactionID string, participantID ID, transactionState transaction.State) <-chan PersistResult
}

type internalStatePersisterAdapter[ID comparable] struct {
	transactionStatePersister TransactionStatePersister[ID]
}

func (a internalStatePersisterAdapter[ID]) PersistState(ctx context.Context, txID string, participantID ID, txState transaction.State) <-chan PersistResult {
	return a.transactionStatePersister.PersistState(ctx, txID, participantID, toExposed(txState))
}

func toExposed(txState transaction.State) TransactionState {
	switch txState {
	case transaction.NotStarted:
		return TransactionNotStarted
	case transaction.Prepared:
		return TransactionPrepared
	case transaction.PrepareFailed:
		return TransactionPrepareFailed
	case transaction.Committed:
		return TransactionCommitted
	case transaction.RolledBack:
		return TransactionRolledBack
	default:
		panic("unsupported transaction.State")
	}
}

// PersistResult is returned by TransactionStatePersister.PersistState over a channel.
// The coordinator calls either Commit or Rollback exactly once, depending on
// whether the corresponding network operation to the participant succeeded.
//
// Err, if non-nil, indicates that the persist attempt itself failed before
// Commit or Rollback can be called. In that case, Commit and Rollback must
// not be called.
type PersistResult struct {
	// Commit finalizes the persisted state change. Called when the operation
	// to the participant succeeded.
	Commit func() error
	// Rollback undoes the persisted state change. Called when the operation
	// to the participant failed.
	Rollback func() error
	// Err is set when the persistence operation itself failed. When non-nil,
	// neither Commit nor Rollback should be called.
	Err error
}

// PreparePayload is the opaque data sent to a participant during the Prepare
// phase. Implementations define the concrete type; the coordinator treats it
// as a black box and forwards it unchanged.
type PreparePayload interface{}

// Client is the interface the coordinator uses to communicate with a single
// two-phase commit participant. Each method corresponds to one phase message.
type Client interface {
	// PrepareTransaction asks the participant to prepare the given transaction.
	// payload carries the operation-specific data needed by the participant.
	// Returns an error if the participant cannot prepare (vote abort).
	PrepareTransaction(ctx context.Context, transactionID string, payload PreparePayload) error
	// CommitTransaction instructs the participant to commit the previously
	// prepared transaction. Called only after all participants have voted yes.
	CommitTransaction(ctx context.Context, transactionID string) error
	// RollbackTransaction instructs the participant to roll back the previously
	// prepared (or never-prepared) transaction.
	RollbackTransaction(ctx context.Context, transactionID string) error
}

type internalClientAdapter struct {
	client Client
}

func (a internalClientAdapter) PrepareTransaction(ctx context.Context, txID string, payload participant.PreparePayload) error {
	return a.client.PrepareTransaction(ctx, txID, payload)
}

func (a internalClientAdapter) CommitTransaction(ctx context.Context, txID string) error {
	return a.client.CommitTransaction(ctx, txID)
}

func (a internalClientAdapter) RollbackTransaction(ctx context.Context, txID string) error {
	return a.client.RollbackTransaction(ctx, txID)
}

func adaptForInternal[ID comparable](newClientFunc func(participantID ID) (Client, error)) func(participantID ID) (participant.Client, error) {
	return func(participantID ID) (participant.Client, error) {
		client, err := newClientFunc(participantID)
		if err != nil {
			return internalClientAdapter{}, fmt.Errorf("adapting client for internal: %w", err)
		}
		return internalClientAdapter{client: client}, nil
	}
}
