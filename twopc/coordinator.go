package twopc

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/participant"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/state"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

// Coordinator orchestrates a two-phase commit protocol across multiple participants.
// It manages the full lifecycle of a distributed transaction: preparing all participants,
// then either committing or rolling back based on the outcomes of the prepare phase.
//
// ID is the type used to uniquely identify each participant client.
type Coordinator[ID comparable] struct {
	stateLoader               state.Loader[ID]
	transactionStatePersister transactionStatePersister[ID]
	participantRegistrar      participant.Registrar[ID]
}

// NewCoordinator creates a new Coordinator with the provided dependencies.
//
// transactionStateChecker is used on startup to recover the current state of
// an in-flight transaction (e.g. after a coordinator crash).
//
// transactionStatePersister is called after each phase transition to durably record the
// new state before the result is considered final. It returns a channel that
// delivers a PersistResult, which must be committed or rolled back depending
// on whether the operation to the participant succeeded.
//
// newClientFunc is called once per participant ID to construct the gRPC (or
// other transport) client used to send Prepare, Commit, and Rollback calls.
func NewCoordinator[ID comparable](
	transactionStateChecker TransactionStateChecker[ID],
	transactionStatePersister TransactionStatePersister[ID],
	newClientFunc func(participantID ID) (Client, error),
) *Coordinator[ID] {
	return &Coordinator[ID]{
		stateLoader:               state.NewLoader(internalTransactionStateCheckerAdapter[ID]{transactionStateChecker: transactionStateChecker}),
		transactionStatePersister: internalStatePersisterAdapter[ID]{transactionStatePersister: transactionStatePersister},
		participantRegistrar:      participant.NewRegistrar[ID](adaptForInternal(newClientFunc)),
	}
}

// Execute runs the two-phase commit protocol for the given distributed transaction.
//
// It drives all participant transactions through the Prepare → Commit (or Rollback)
// state machine concurrently. If the context is canceled between phases, the method
// returns immediately with a joined error that includes the cancellation cause.
//
// Errors from individual participants are accumulated and returned as a single joined
// error. A nil return means all participants reached a terminal committed state successfully.
func (oh Coordinator[ID]) Execute(ctx context.Context, distributedTransaction DistributedTransaction[ID]) Result {
	initialState, err := oh.stateLoader.LoadState(distributedTransaction.TransactionID, participantIDs(distributedTransaction.Transactions))
	if err != nil {
		return Result{
			err:     err,
			outcome: OutcomeInconsistent,
		}
	}

	initialOperations := toInitialOperations(distributedTransaction.Transactions)

	return oh.runTransactionLoop(ctx, distributedTransaction.TransactionID, initialState, initialOperations)
}

func participantIDs[ID comparable](trs []Transaction[ID]) []ID {
	ids := make([]ID, 0, len(trs))
	for _, tr := range trs {
		ids = append(ids, tr.ParticipantID)
	}
	return ids
}

func toInitialOperations[ID comparable](txs []Transaction[ID]) []operation[ID] {
	ops := make([]operation[ID], 0, len(txs))
	for _, tx := range txs {
		ops = append(ops, newInitialOperation(tx.ParticipantID, tx.Payload))
	}
	return ops
}

func (oh Coordinator[ID]) runTransactionLoop(
	ctx context.Context,
	txID string,
	state state.State[ID],
	ops []operation[ID],
) Result {
	var successful, failed []operation[ID]
	var allErrs []error

	for !state.IsTerminal() {
		if err := ctx.Err(); err != nil {
			return Result{
				err:     errors.Join(append(allErrs, err)...),
				outcome: outcome(state),
			}
		}

		ops = nextOperations(state, ops)

		var err error
		successful, failed, err = oh.executeRound(ctx, txID, ops, successful[:0], failed[:0])

		if err != nil {
			allErrs = append(allErrs, err)
		}

		nextState(state, successful, failed)
	}

	return Result{
		err:     errors.Join(allErrs...),
		outcome: outcome(state),
	}
}

func nextOperations[ID comparable](s state.State[ID], ops []operation[ID]) []operation[ID] {
	payloadByParticipantID := toPayloadByParticipantID(ops)
	nextTrs := s.NextTransitions()
	return toOperations(nextTrs, payloadByParticipantID)
}

func toPayloadByParticipantID[ID comparable](ops []operation[ID]) map[ID]participant.PreparePayload {
	payloadByParticipantID := make(map[ID]participant.PreparePayload)
	for _, op := range ops {
		payloadByParticipantID[op.participantID] = op.payload
	}
	return payloadByParticipantID
}

func toOperations[ID comparable](
	nextTrs []state.Transition[ID],
	payloadByParticipantID map[ID]participant.PreparePayload,
) []operation[ID] {
	nextOps := make([]operation[ID], 0, len(nextTrs))
	for _, nextTr := range nextTrs {
		nextOp := operation[ID]{
			participantID: nextTr.ParticipantID(),
			payload:       payloadByParticipantID[nextTr.ParticipantID()],
			sourceState:   nextTr.SourceState(),
			targetState:   nextTr.TargetState(),
		}
		nextOps = append(nextOps, nextOp)
	}
	return nextOps
}

func nextState[ID comparable](s state.State[ID], successful, failed []operation[ID]) {
	s.NextState(toTransitions(successful), toTransitions(failed))
}

func toTransitions[ID comparable](ops []operation[ID]) []state.Transition[ID] {
	trs := make([]state.Transition[ID], 0, len(ops))
	for _, op := range ops {
		trs = append(trs, op.toTransition())
	}
	return trs
}

func outcome[ID comparable](s state.State[ID]) Outcome {
	switch {
	case s.IsRolledBack():
		return OutcomeRolledBack
	case s.IsCommitted():
		return OutcomeCommitted
	default:
		return OutcomeInconsistent
	}
}

// Result holds the outcome of a two-phase commit execution.
// Callers should inspect Outcome first to determine the final transaction state,
// then check Err for any infrastructure or participant errors that occurred during execution.
// A non-nil Err does not imply an inconsistent state — for example, a participant may have
// returned a transient error while the transaction still reached a terminal state.
type Result struct {
	err     error
	outcome Outcome
}

// Err returns any errors accumulated during execution.
// These may originate from participant RPCs, state persistence, or context cancellation.
// A nil error alongside OutcomeCommitted or OutcomeRolledBack means the transaction
// completed cleanly. A non-nil error alongside a terminal outcome means the transaction
// reached that outcome despite encountering errors along the way.
func (r Result) Err() error {
	return r.err
}

// Outcome returns the terminal state reached by the distributed transaction.
func (r Result) Outcome() Outcome {
	return r.outcome
}

// Outcome represents the terminal state reached by a distributed transaction
// after Execute returns.
type Outcome int

const (
	// OutcomeInconsistent means the transaction did not reach a clean terminal state.
	// This occurs either when the persisted transaction state could not be read on startup,
	// or when Execute encounters an unrecoverable infrastructure failure before all participants
	// could reach the same terminal state — for example, some participants committed while
	// others did not, or some rolled back while others did not.
	// The caller must invoke Execute again with the same transaction ID to drive all
	// participants to a terminal state.
	OutcomeInconsistent = iota

	// OutcomeCommitted means all participants successfully prepared and committed.
	// The transaction is durably complete.
	OutcomeCommitted

	// OutcomeRolledBack means all participants have been rolled back.
	// This occurs when at least one participant failed the prepare phase,
	// causing the coordinator to roll back all participants before any commit was attempted.
	OutcomeRolledBack
)

func (oh Coordinator[ID]) executeRound(
	ctx context.Context,
	txID string,
	ops, successful, failed []operation[ID],
) ([]operation[ID], []operation[ID], error) {
	resultCh := make(chan operationResult[ID], len(ops))
	oh.runOperationsConcurrently(ctx, resultCh, txID, ops)

	var errs []error
	for result := range resultCh {
		if result.operationErr != nil {
			failed = append(failed, result.operation)
			errs = append(errs, result.operationErr)
		} else {
			successful = append(successful, result.operation)
		}
	}

	var err error
	if len(errs) > 0 {
		err = errors.Join(errs...)
	}

	return successful, failed, err
}

func (oh Coordinator[ID]) runOperationsConcurrently(
	ctx context.Context,
	resultCh chan<- operationResult[ID],
	txID string,
	ops []operation[ID],
) {
	var wg sync.WaitGroup

	for _, op := range ops {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := oh.runOperation(ctx, txID, op)
			resultCh <- operationResult[ID]{operationErr: err, operation: op}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()
}

type operationResult[ID comparable] struct {
	operationErr error
	operation    operation[ID]
}

func (oh Coordinator[ID]) runOperation(ctx context.Context, txID string, op operation[ID]) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	operationDoneCh := oh.sendOperation(ctx, txID, op)

	ctx, persistCancel := context.WithTimeout(ctx, persistStateTimeout)
	defer persistCancel()
	persistResultCh := oh.transactionStatePersister.PersistState(ctx, txID, op.participantID, op.targetState)

	err := <-operationDoneCh
	if err != nil {
		cancel()
	}
	result := <-persistResultCh
	if result.Err != nil {
		if err != nil {
			return errors.Join(err, result.Err)
		}
		return result.Err
	}
	if err != nil {
		rollbackErr := result.Rollback()
		if rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}
		return err
	}
	return result.Commit()
}

const persistStateTimeout = 5 * time.Second

func (oh Coordinator[ID]) sendOperation(ctx context.Context, txID string, op operation[ID]) <-chan error {
	operationDoneCh := make(chan error)

	go func() {
		operationDoneCh <- oh._sendOperation(ctx, txID, op)
	}()

	return operationDoneCh
}

func (oh Coordinator[ID]) _sendOperation(ctx context.Context, txID string, op operation[ID]) error {
	ctx, cancel := context.WithTimeout(ctx, sendOperationTimeout)
	defer cancel()

	c, err := oh.participantRegistrar.GetClient(op.participantID)
	if err != nil {
		return err
	}
	switch op.targetState {
	case transaction.Prepared:
		return c.PrepareTransaction(ctx, txID, op.payload)
	case transaction.Committed:
		return c.CommitTransaction(ctx, txID)
	case transaction.RolledBack:
		return c.RollbackTransaction(ctx, txID)
	default:
		panic("unknown operation type")
	}
}

const sendOperationTimeout = 5 * time.Second

type operation[ID comparable] struct {
	participantID ID
	payload       participant.PreparePayload
	sourceState   transaction.State
	targetState   transaction.State
}

func newInitialOperation[ID comparable](participantID ID, payload participant.PreparePayload) operation[ID] {
	return operation[ID]{
		participantID: participantID,
		payload:       payload,
		sourceState:   transaction.NotStarted,
		targetState:   transaction.Prepared,
	}
}

func (o operation[ID]) toTransition() state.Transition[ID] {
	switch {
	case o.sourceState == transaction.NotStarted && o.targetState == transaction.Prepared:
		return state.PrepareTransition(o.participantID)
	case o.sourceState == transaction.Prepared && o.targetState == transaction.Committed:
		return state.CommitTransition(o.participantID)
	case o.targetState == transaction.RolledBack:
		return state.RollbackTransition(o.participantID, o.sourceState)
	default:
		panic("unreachable")
	}
}
