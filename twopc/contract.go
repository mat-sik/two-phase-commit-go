package twopc

import (
	"context"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/client"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

// TransactionStateChecker is used by the Coordinator to recover the persisted
// state of a distributed transaction, typically on startup after a crash.
// The returned map associates each participant ID with its last known state.
type TransactionStateChecker[ID comparable] interface {
	Check(transactionID string) map[ID]TransactionState
}

// TransactionState represents the lifecycle state of a single participant's
// transaction within the two-phase commit protocol.
type TransactionState int

const (
	// NotStarted means the participant has not yet been contacted.
	NotStarted TransactionState = iota
	// Prepared means the participant has successfully completed the prepare phase
	// and is ready to commit.
	Prepared
	// PrepareFailed means the participant responded with a failure during the
	// prepare phase, triggering a global rollback.
	PrepareFailed
	// Committed means the participant has durably committed the transaction.
	Committed
	// RolledBack means the participant has rolled back the transaction.
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
	PersistState(ctx context.Context, transactionID string, clientID ID, transactionState TransactionState) <-chan PersistResult
}

type transactionStatePersister[ID comparable] interface {
	PersistState(ctx context.Context, transactionID string, clientID ID, transactionState transaction.State) <-chan PersistResult
}

type internalStatePersisterAdapter[ID comparable] struct {
	transactionStatePersister TransactionStatePersister[ID]
}

func (i internalStatePersisterAdapter[ID]) PersistState(ctx context.Context, transactionID string, clientID ID, transactionState transaction.State) <-chan PersistResult {
	return i.transactionStatePersister.PersistState(ctx, transactionID, clientID, toExposed(transactionState))
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
