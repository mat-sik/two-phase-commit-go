package twopc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/participant"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/retry"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/state"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

// Coordinator orchestrates a two-phase commit protocol across multiple participants.
// It manages the full lifecycle of a distributed transaction: preparing all participants,
// then either committing or rolling back based on the outcomes of the prepare phase.
//
// ID is the type used to uniquely identify each participant client.
type Coordinator[ID comparable] struct {
	config                    config
	stateLoader               state.Loader[ID]
	transactionStatePersister transactionStatePersister[ID]
	participantRegistrar      participant.Registrar[ID]
}

// PersistenceConfig aggregates required interfaces for transaction state persistence.
type PersistenceConfig[ID comparable] struct {
	TransactionStateChecker   TransactionStateChecker[ID]
	TransactionStatePersister TransactionStatePersister[ID]
}

// ClientConfig holds the client construction strategy for participant registration.
type ClientConfig[ID comparable] struct {
	// NewClientFunc is called to construct a client for an unknown participant ID.
	// It may be nil if all participant IDs are covered by Clients map.
	NewClientFunc func(participantID ID) (Client, error)
	Clients       map[ID]Client
}

// NewCoordinator creates a new Coordinator with the provided dependencies.
func NewCoordinator[ID comparable](
	persistenceConfig PersistenceConfig[ID],
	clientConfig ClientConfig[ID],
	opts ...Option,
) *Coordinator[ID] {
	stateChecker := internalTransactionStateCheckerAdapter[ID]{transactionStateChecker: persistenceConfig.TransactionStateChecker}
	statePersister := internalStatePersisterAdapter[ID]{transactionStatePersister: persistenceConfig.TransactionStatePersister}
	return &Coordinator[ID]{
		config:                    newConfig(opts...),
		stateLoader:               state.NewLoader(stateChecker),
		transactionStatePersister: statePersister,
		participantRegistrar:      newParticipantRegistrar(clientConfig),
	}
}

func newParticipantRegistrar[ID comparable](clientConfig ClientConfig[ID]) participant.Registrar[ID] {
	newClientFunc := adaptForInternal(clientConfig.NewClientFunc)
	clients := make(map[ID]participant.Client, len(clientConfig.Clients))
	for participantID, client := range clientConfig.Clients {
		clients[participantID] = internalClientAdapter{client: client}
	}
	return participant.NewRegistrar(newClientFunc, clients)
}

func (c Coordinator[ID]) newExecutor(ctx context.Context, initialState state.State[ID]) executor[ID] {
	return executor[ID]{
		config:                    c.config,
		state:                     initialState,
		participantFailureCounter: participant.NewFailureCounter[ID](),
		participantRegistrar:      c.participantRegistrar,
		persister:                 newPersister(ctx, c.transactionStatePersister),
	}
}

// Execute runs the two-phase commit protocol for the given distributed transaction.
//
// It drives all participant transactions through the Prepare → Commit (or Rollback)
// state machine concurrently. If the context is canceled during execution, the method
// returns immediately with an error that includes the cancellation cause.
//
// Errors from participant operations and transaction-state persistence are accumulated
// and returned as a single joined error. Persistence failures do not affect protocol
// execution and may be returned even when the transaction reaches a terminal state.
//
// Callers should inspect the returned Result.Outcome() to determine the final
// transaction state and Result.Err() for any errors encountered while executing it.
func (c Coordinator[ID]) Execute(ctx context.Context, distributedTransaction DistributedTransaction[ID]) Result {
	c.assertCorrectConfiguration(participantIDs(distributedTransaction.Transactions))

	initialState, err := c.stateLoader.LoadState(ctx, distributedTransaction.TransactionID, participantIDs(distributedTransaction.Transactions))
	if err != nil {
		return Result{
			err:     err,
			outcome: OutcomeInconsistent,
		}
	}

	initialOperations := toInitialOperations(distributedTransaction.Transactions)

	exec := c.newExecutor(ctx, initialState)

	return exec.runTransactionLoop(ctx, distributedTransaction.TransactionID, initialOperations)
}

func (c Coordinator[ID]) assertCorrectConfiguration(participantIDs []ID) {
	for _, participantID := range participantIDs {
		_, err := c.participantRegistrar.GetClient(participantID)
		if errors.Is(err, participant.ErrInvalidClientConfig) {
			panic(err)
		}
	}
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

type executor[ID comparable] struct {
	config                    config
	state                     state.State[ID]
	participantFailureCounter *participant.AttemptCounter[ID]
	participantRegistrar      participant.Registrar[ID]
	persister                 *persister[ID]
}

func (e executor[ID]) runTransactionLoop(ctx context.Context, txID string, ops []operation[ID]) (result Result) {
	defer func() {
		if err := e.persister.stop(); err != nil {
			result.err = errors.Join(result.err, err)
		}
	}()

	var successful, failed []operation[ID]
	var errs []error

	for !e.state.IsTerminal() {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			return Result{
				err:     errors.Join(errs...),
				outcome: outcome(e.state),
			}
		}

		ops = e.nextOperations(ops)

		var err error
		successful, failed, err = e.executeRound(ctx, txID, ops, successful[:0], failed[:0])

		if err != nil {
			errs = append(errs, err)
		}

		e.nextState(successful, failed)
	}

	return Result{
		err:     errors.Join(errs...),
		outcome: outcome(e.state),
	}
}

func (e executor[ID]) nextOperations(ops []operation[ID]) []operation[ID] {
	payloadByParticipantID := toPayloadByParticipantID(ops)
	nextTrs := e.state.NextTransitions()
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

func (e executor[ID]) nextState(successful, failed []operation[ID]) {
	e.state.NextState(toTransitions(successful), toTransitions(failed))
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

func (e executor[ID]) executeRound(
	ctx context.Context,
	txID string,
	ops, successful, failed []operation[ID],
) ([]operation[ID], []operation[ID], error) {
	resultCh := make(chan operationResult[ID], len(ops))
	e.sendOperationsConcurrently(ctx, resultCh, txID, ops)

	var errs []error
	for result := range resultCh {
		if result.err != nil {
			errs = append(errs, result.err)
		}
		if result.err != nil {
			failed = append(failed, result.operation)
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

func (e executor[ID]) sendOperationsConcurrently(
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
			err := e.withBackoff(ctx, op.participantID, func() error {
				return e.sendOperation(ctx, txID, op)
			})
			if err == nil {
				e.persister.enqueuePersistState(ctx, txID, op.participantID, op.targetState)
			}
			resultCh <- operationResult[ID]{err: err, operation: op}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()
}

type operationResult[ID comparable] struct {
	err       error
	operation operation[ID]
}

func (e executor[ID]) withBackoff(ctx context.Context, participantID ID, workFunc func() error) error {
	if attempt := e.participantFailureCounter.Attempt(participantID); attempt > 0 {
		backoffWait(ctx, e.config, attempt)
	}
	if err := workFunc(); err != nil {
		e.participantFailureCounter.Fail(participantID)
		return fmt.Errorf("backing off participant %v: %w", participantID, err)
	}
	e.participantFailureCounter.Success(participantID)
	return nil
}

func backoffWait(ctx context.Context, cfg config, attempt int) {
	backoff := retry.NewBackoff(cfg.backoffBase, cfg.backoffMax, cfg.backoffFactor)
	backoff.Wait(ctx, attempt)
}

func (e executor[ID]) sendOperation(ctx context.Context, txID string, op operation[ID]) error {
	ctx, cancel := context.WithTimeout(ctx, e.config.sendOperationTimeout)
	defer cancel()

	client, err := e.participantRegistrar.GetClient(op.participantID)
	if err != nil {
		return fmt.Errorf("getting %v client: %w", op.participantID, err)
	}

	switch op.targetState {
	case transaction.Prepared:
		if err = client.PrepareTransaction(ctx, txID, op.payload); err != nil {
			return fmt.Errorf("preparing tx %s payload %v: %w", txID, op.payload, err)
		}
		return nil
	case transaction.Committed:
		if err = client.CommitTransaction(ctx, txID); err != nil {
			return fmt.Errorf("committing tx %s: %w", txID, err)
		}
		return nil
	case transaction.RolledBack:
		if err = client.RollbackTransaction(ctx, txID); err != nil {
			return fmt.Errorf("rolling back tx %s: %w", txID, err)
		}
		return nil
	default:
		panic("unknown operation type")
	}
}

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
		panic("logic should prohibit this")
	}
}
