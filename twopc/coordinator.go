package twopc

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/client"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/state"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

type Coordinator struct {
	stateLoader     state.Loader
	statePersister  StatePersister
	clientRegistrar client.Registrar
}

func NewCoordinator(
	stateLoader state.Loader,
	statePersister StatePersister,
	newClientFunc func(clientID client.ID) (client.Client, error),
) *Coordinator {
	return &Coordinator{
		stateLoader:     stateLoader,
		statePersister:  statePersister,
		clientRegistrar: client.NewRegistrar(newClientFunc),
	}
}

type StatePersister interface {
	PersistState(ctx context.Context, transactionID string, clientID client.ID, transactionState transaction.State) <-chan PersistResult
}

type PersistResult struct {
	Commit   func() error
	Rollback func() error
	Err      error
}

func (oh Coordinator) Execute(ctx context.Context, distributedTransaction DistributedTransaction) error {
	return oh.execute(ctx, distributedTransaction.TransactionID, distributedTransaction.Transactions)
}

func (oh Coordinator) execute(ctx context.Context, transactionID string, transactions []Transaction) error {
	initialState := oh.stateLoader.LoadState(transactionID, clientIDS(transactions))
	operations := toInitialOperations(transactions)

	var allErrs []error
	var successfulOperations []operation
	var failedOperations []operation
	for currState := initialState; !currState.IsTerminalState(len(transactions)); currState = nextState(currState, successfulOperations, failedOperations) {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(allErrs, err)...)
		}

		operations = nextOperations(currState, operations)

		resultCh := make(chan operationResult, len(operations))
		oh.runOperationsConcurrently(ctx, resultCh, transactionID, operations)

		successfulOperations = successfulOperations[:0]
		failedOperations = failedOperations[:0]
		var errs []error
		for result := range resultCh {
			if result.operationErr != nil {
				failedOperations = append(failedOperations, result.operation)
				errs = append(errs, result.operationErr)
			} else {
				successfulOperations = append(successfulOperations, result.operation)
			}
		}

		if len(errs) > 0 {
			allErrs = append(allErrs, errors.Join(errs...))
		}
	}
	return errors.Join(allErrs...)
}

func clientIDS(transactions []Transaction) []client.ID {
	ids := make([]client.ID, 0, len(transactions))
	for _, tr := range transactions {
		ids = append(ids, tr.ClientID)
	}
	return ids
}

func toInitialOperations(transactions []Transaction) []operation {
	ops := make([]operation, 0, len(transactions))
	for _, tx := range transactions {
		ops = append(ops, newInitialOperation(tx.ClientID, tx.Payload))
	}
	return ops
}

func nextState(state state.State, successfulOperations, failedOperations []operation) state.State {
	return state.NextState(toTransitions(successfulOperations), toTransitions(failedOperations))
}

func toTransitions(ops []operation) []state.Transition {
	trs := make([]state.Transition, 0, len(ops))
	for _, op := range ops {
		trs = append(trs, op.toTransition())
	}
	return trs
}

func nextOperations(currentState state.State, ops []operation) []operation {
	payloadByClientID, transitions := toTransitionsWithPayloads(ops)
	nextTrs := currentState.NextTransitions(transitions)
	return toOperations(nextTrs, payloadByClientID)
}

func toTransitionsWithPayloads(ops []operation) (map[client.ID]client.PreparePayload, []state.Transition) {
	payloadByClientID := make(map[client.ID]client.PreparePayload)
	transitions := make([]state.Transition, 0, len(ops))
	for _, op := range ops {
		transitions = append(transitions, op.toTransition())
		payloadByClientID[op.clientID] = op.payload
	}
	return payloadByClientID, transitions
}

func toOperations(
	nextTransitions []state.Transition,
	payloadByClientID map[client.ID]client.PreparePayload,
) []operation {
	nextOps := make([]operation, 0, len(nextTransitions))
	for _, nextTr := range nextTransitions {
		nextOp := operation{
			clientID:    nextTr.GetClientID(),
			payload:     payloadByClientID[nextTr.GetClientID()],
			sourceState: nextTr.GetSourceState(),
			targetState: nextTr.GetTargetState(),
		}
		nextOps = append(nextOps, nextOp)
	}
	return nextOps
}

func (oh Coordinator) runOperationsConcurrently(
	ctx context.Context,
	resultCh chan<- operationResult,
	transactionID string,
	operations []operation,
) {
	var wg sync.WaitGroup

	for _, op := range operations {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := oh.runOperation(ctx, transactionID, op)
			resultCh <- operationResult{operationErr: err, operation: op}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()
}

type operationResult struct {
	operationErr error
	operation    operation
}

func (oh Coordinator) runOperation(ctx context.Context, transactionID string, operation operation) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	operationSentCh := oh.sendOperation(ctx, transactionID, operation)

	ctx, persistCancel := context.WithTimeout(ctx, persistStateTimeout)
	defer persistCancel()
	persistResultCh := oh.statePersister.PersistState(ctx, transactionID, operation.ClientIdentifier(), operation.targetState)

	err := <-operationSentCh
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

func (oh Coordinator) sendOperation(ctx context.Context, transactionID string, operation operation) <-chan error {
	operationDoneCh := make(chan error)

	go func() {
		operationDoneCh <- oh._sendOperation(ctx, transactionID, operation)
	}()

	return operationDoneCh
}

func (oh Coordinator) _sendOperation(ctx context.Context, transactionID string, operation operation) error {
	ctx, cancel := context.WithTimeout(ctx, sendOperationTimeout)
	defer cancel()

	c, err := oh.clientRegistrar.GetClient(operation.clientID)
	if err != nil {
		return err
	}
	switch operation.targetState {
	case transaction.Prepared:
		return c.PrepareTransaction(ctx, transactionID, operation.payload)
	case transaction.Committed:
		return c.CommitTransaction(ctx, transactionID)
	case transaction.RolledBack:
		return c.RollbackTransaction(ctx, transactionID)
	default:
		panic(errors.New("unknown operation type"))
	}
}

const sendOperationTimeout = 5 * time.Second

type operation struct {
	clientID    client.ID
	payload     client.PreparePayload
	sourceState transaction.State
	targetState transaction.State
}

func newInitialOperation(clientID client.ID, payload client.PreparePayload) operation {
	return operation{
		clientID:    clientID,
		payload:     payload,
		sourceState: transaction.NotStarted,
		targetState: transaction.Prepared,
	}
}

func (o operation) toTransition() state.Transition {
	return state.NewTransition(o.clientID, o.sourceState, o.targetState)
}

func (o operation) ClientIdentifier() client.ID {
	return o.clientID
}
