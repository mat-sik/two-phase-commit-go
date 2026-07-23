package twopc

import (
	"context"
	"fmt"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/participant"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

// TransactionStateChecker is used during recovery to load the last known
// state of each participant in a distributed transaction.
//
// The coordinator uses this information to reconstruct transaction progress
// and resume execution after a crash or restart. The returned map associates
// participant IDs with their most recently persisted states.
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

// TransactionStatePersister durably records participant state transitions.
//
// The coordinator invokes PersistState after a participant successfully
// completes an operation (Prepare, Commit, or Rollback). Persistence is
// performed asynchronously and does not influence the outcome of the
// two-phase commit protocol.
//
// Implementations should write the participant's latest state to durable
// storage (for example, a database) so that a future coordinator instance
// can recover and resume an interrupted transaction.
//
// Persistence failures are collected and returned as part of the final
// Result.Err(), but they do not prevent the coordinator from continuing
// protocol execution. Participant state is considered the source of truth,
// persisted coordinator state exists solely for recovery.
//
// Participant operations must therefore be idempotent. If persistence is
// missing or stale and the coordinator crashes, recovery may resend an
// operation that was already applied. Participants must safely tolerate
// such duplicates.
type TransactionStatePersister[ID comparable] interface {
	PersistState(ctx context.Context, transactionID string, participantID ID, transactionState TransactionState) error
}

type transactionStatePersister[ID comparable] interface {
	PersistState(ctx context.Context, transactionID string, participantID ID, transactionState transaction.State) error
}

type internalStatePersisterAdapter[ID comparable] struct {
	transactionStatePersister TransactionStatePersister[ID]
}

func (a internalStatePersisterAdapter[ID]) PersistState(ctx context.Context, txID string, participantID ID, txState transaction.State) error {
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

// PreparePayload is the opaque data sent to a participant during the Prepare
// phase. Implementations define the concrete type; the coordinator treats it
// as a black box and forwards it unchanged.
type PreparePayload interface{}

// Client is the interface the coordinator uses to communicate with a single
// two-phase commit participant. Each method corresponds to one phase message.
// Endpoints called by these methods must be idempotent, since the coordinator
// may retry a call after a timeout or transient failure without knowing
// whether the participant already processed it.
type Client interface {
	// PrepareTransaction asks the participant to prepare the given transaction.
	// payload carries the operation-specific data needed by the participant.
	// Returns an error if the participant cannot prepare (vote abort).
	PrepareTransaction(ctx context.Context, transactionID string, payload PreparePayload) error
	// CommitTransaction instructs the participant to commit the previously
	// prepared transaction. Called only after all participants have voted yes.
	CommitTransaction(ctx context.Context, transactionID string) error
	// RollbackTransaction instructs the participant to roll back the previously
	// prepared transaction.
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
