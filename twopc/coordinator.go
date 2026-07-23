package twopc

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/participant"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/retry"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/state"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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

	exec := c.newExecutor(ctx, initialState, distributedTransaction)
	return exec.runTransactionLoop(ctx)
}

func (c Coordinator[ID]) newExecutor(
	ctx context.Context,
	initialState state.State[ID],
	distributedTransaction DistributedTransaction[ID],
) executor[ID] {
	return executor[ID]{
		config:                    c.config,
		state:                     initialState,
		transactionID:             distributedTransaction.TransactionID,
		payloadByParticipantID:    newPayloadByParticipantID(distributedTransaction),
		participantFailureCounter: participant.NewFailureCounter[ID](),
		participantRegistrar:      c.participantRegistrar,
		persister:                 newPersister(ctx, c.transactionStatePersister, c.config.tracer),
		tracer:                    c.config.tracer,
	}
}

func newPayloadByParticipantID[ID comparable](
	distributedTransaction DistributedTransaction[ID],
) map[ID]participant.PreparePayload {
	payloadByParticipant := make(map[ID]participant.PreparePayload, len(distributedTransaction.Transactions))
	for _, tx := range distributedTransaction.Transactions {
		payloadByParticipant[tx.ParticipantID] = tx.Payload
	}
	return payloadByParticipant
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

type executor[ID comparable] struct {
	config                    config
	state                     state.State[ID]
	transactionID             string
	payloadByParticipantID    map[ID]participant.PreparePayload
	participantFailureCounter *participant.AttemptCounter[ID]
	participantRegistrar      participant.Registrar[ID]
	persister                 *persister[ID]
	tracer                    trace.Tracer
}

func (e executor[ID]) runTransactionLoop(ctx context.Context) (result Result) {
	var span trace.Span
	ctx, span = runTransactionLoopSpan(ctx, e.tracer, e.transactionID, e.payloadByParticipantID)

	defer func() {
		if err := e.persister.stop(); err != nil {
			result.err = errors.Join(result.err, err)
		}
		recordOutcome(ctx, result)
		span.End()
	}()

	var successful, failed []operation[ID]
	var errs []error

	for !e.state.IsTerminal() {
		if err := ctx.Err(); err != nil {
			span.AddEvent("abandoning due to context err")
			return Result{
				err:     errors.Join(append(errs, err)...),
				outcome: outcome(e.state),
			}
		}

		ops := e.nextOperations()

		var err error
		successful, failed, err = e.executeRound(ctx, ops, successful[:0], failed[:0])

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

func (e executor[ID]) nextOperations() []operation[ID] {
	nextTrs := e.state.NextTransitions()
	return toOperations(nextTrs, e.payloadByParticipantID)
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
	case s.IsCommitted():
		return OutcomeSuccess
	case s.IsFailed():
		return OutcomeFailed
	default:
		return OutcomeInconsistent
	}
}

func runTransactionLoopSpan[ID comparable](
	ctx context.Context,
	tracer trace.Tracer,
	txID string,
	payloadByParticipantID map[ID]participant.PreparePayload,
) (context.Context, trace.Span) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "distributed-transaction-loop")

	attrs := make([]attribute.KeyValue, 0, len(payloadByParticipantID)+1)

	attrs = append(attrs,
		attribute.String("transaction.id", txID),
	)

	for participantID, payload := range payloadByParticipantID {
		attrs = append(attrs, attribute.String(
			fmt.Sprintf("participant.%v.payload", participantID),
			fmt.Sprintf("%v", payload),
		))
	}

	span.SetAttributes(attrs...)

	return ctx, span
}

func recordOutcome(ctx context.Context, result Result) {
	span := trace.SpanFromContext(ctx)
	if result.err != nil {
		span.RecordError(result.Err(),
			trace.WithAttributes(attribute.Int("transaction.outcome", int(result.Outcome()))),
		)
		if result.Outcome() == OutcomeFailed {
			span.SetStatus(codes.Error, "transaction failed")
		} else if result.Outcome() == OutcomeInconsistent {
			span.SetStatus(codes.Error, "transaction failed and participants left in inconsistent state")
		}
	}
}

// Result holds the outcome of a two-phase commit execution.
// Callers should inspect Outcome first to determine the final transaction state,
// then check Err for any infrastructure or participant errors that occurred during execution.
// A non-nil Err does not imply an inconsistent state - for example, a participant may have
// returned a transient error while the transaction still reached a terminal state.
type Result struct {
	outcome Outcome
	err     error
}

// Err returns any errors accumulated during execution.
// These may originate from participant RPCs, state persistence, or context cancellation.
// A nil error alongside OutcomeSuccess or OutcomeFailed means the transaction
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
	// could reach the same terminal state - for example, some participants committed while
	// others did not, or some rolled back while others did not.
	// The caller must invoke Execute again with the same transaction ID to drive all
	// participants to a terminal state.
	OutcomeInconsistent = iota

	// OutcomeSuccess means all participants successfully prepared and committed their transaction.
	// This is a successful terminal state.
	OutcomeSuccess

	// OutcomeFailed means that the state is consistent but distributed transaction failed.
	// occurs when:
	// - all participant transactions have been rolled back
	// - all participants failed to prepare transaction
	// - some failed to prepare transaction and some have its transaction rolled back
	// This is an unsuccessful terminal state.
	OutcomeFailed
)

func (e executor[ID]) executeRound(
	ctx context.Context,
	ops, successful, failed []operation[ID],
) ([]operation[ID], []operation[ID], error) {
	var span trace.Span
	ctx, span = executeRoundSpan(ctx, e.tracer, ops)
	defer span.End()

	resultCh := make(chan operationResult[ID], len(ops))
	e.sendOperationsConcurrently(ctx, resultCh, ops)

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

func executeRoundSpan[ID comparable](
	ctx context.Context,
	tracer trace.Tracer,
	ops []operation[ID],
) (context.Context, trace.Span) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "execution-round")

	participants := make(map[string]struct{}, len(ops))
	for _, op := range ops {
		participantIDString := fmt.Sprintf("%v", op.participantID)
		participants[participantIDString] = struct{}{}
	}
	span.SetAttributes(
		attribute.StringSlice("operation.participants", slices.Collect(maps.Keys(participants))),
	)

	return ctx, span
}

func (e executor[ID]) sendOperationsConcurrently(
	ctx context.Context,
	resultCh chan<- operationResult[ID],
	ops []operation[ID],
) {
	var wg sync.WaitGroup

	for _, op := range ops {
		wg.Add(1)
		go func() {
			ctx, span := sendOperationConcurrentlySpan(ctx, e.tracer, op)

			defer func() {
				span.End()
				wg.Done()
			}()

			err := e.withBackoff(ctx, op.participantID, func() error {
				return e.sendOperation(ctx, op)
			})
			if err == nil {
				e.persister.enqueuePersistState(ctx, e.transactionID, op.participantID, op.targetState)
			} else {
				span.RecordError(err)
				span.SetStatus(codes.Error, "communication with participant")
			}
			resultCh <- operationResult[ID]{err: err, operation: op}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()
}

func sendOperationConcurrentlySpan[ID comparable](ctx context.Context, tracer trace.Tracer, op operation[ID]) (context.Context, trace.Span) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "sending-operation")
	span.SetAttributes(
		attribute.String("participant.id", fmt.Sprintf("%v", op.participantID)),
		attribute.Int("state.source", int(op.sourceState)),
		attribute.Int("state.target", int(op.targetState)),
	)
	return ctx, span
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

func (e executor[ID]) sendOperation(ctx context.Context, op operation[ID]) error {
	ctx, cancel := context.WithTimeout(ctx, e.config.sendOperationTimeout)
	defer cancel()

	client, err := e.participantRegistrar.GetClient(op.participantID)
	if err != nil {
		return fmt.Errorf("getting %v client: %w", op.participantID, err)
	}

	switch op.targetState {
	case transaction.Prepared:
		if err = client.PrepareTransaction(ctx, e.transactionID, op.payload); err != nil {
			return fmt.Errorf("preparing tx %s payload %v: %w", e.transactionID, op.payload, err)
		}
		return nil
	case transaction.Committed:
		if err = client.CommitTransaction(ctx, e.transactionID); err != nil {
			return fmt.Errorf("committing tx %s: %w", e.transactionID, err)
		}
		return nil
	case transaction.RolledBack:
		if err = client.RollbackTransaction(ctx, e.transactionID); err != nil {
			return fmt.Errorf("rolling back tx %s: %w", e.transactionID, err)
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

func (o operation[ID]) toTransition() state.Transition[ID] {
	switch {
	case o.sourceState == transaction.NotStarted && o.targetState == transaction.Prepared:
		return state.PrepareTransition(o.participantID)
	case o.sourceState == transaction.Prepared && o.targetState == transaction.Committed:
		return state.CommitTransition(o.participantID)
	case o.sourceState == transaction.Prepared && o.targetState == transaction.RolledBack:
		return state.RollbackTransition(o.participantID)
	default:
		panic("logic should prohibit this")
	}
}
