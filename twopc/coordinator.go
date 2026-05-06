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

type Coordinator[ID comparable] struct {
	stateLoader     state.Loader[ID]
	statePersister  StatePersister[ID]
	clientRegistrar client.Registrar[ID]
}

func NewCoordinator[ID comparable](
	transactionStateChecker TransactionStateChecker[ID],
	statePersister StatePersister[ID],
	newClientFunc func(clientID ID) (Client, error),
) *Coordinator[ID] {
	return &Coordinator[ID]{
		stateLoader:     state.NewLoader(transactionStateChecker),
		statePersister:  statePersister,
		clientRegistrar: client.NewRegistrar[ID](adaptForInternalNewClientFunc(newClientFunc)),
	}
}

func (oh Coordinator[ID]) Execute(ctx context.Context, distributedTransaction DistributedTransaction[ID]) error {
	return oh.execute(ctx, distributedTransaction.TransactionID, distributedTransaction.Transactions)
}

func (oh Coordinator[ID]) execute(ctx context.Context, transactionID string, transactions []Transaction[ID]) error {
	initialState := oh.stateLoader.LoadState(transactionID, clientIDS(transactions))
	operations := toInitialOperations(transactions)

	var allErrs []error
	var successfulOperations []operation[ID]
	var failedOperations []operation[ID]
	for currState := initialState; !currState.IsTerminalState(len(transactions)); currState = nextState(currState, successfulOperations, failedOperations) {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(allErrs, err)...)
		}

		operations = nextOperations(currState, operations)

		resultCh := make(chan operationResult[ID], len(operations))
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

func clientIDS[ID comparable](transactions []Transaction[ID]) []ID {
	ids := make([]ID, 0, len(transactions))
	for _, tr := range transactions {
		ids = append(ids, tr.ClientID)
	}
	return ids
}

func toInitialOperations[ID comparable](transactions []Transaction[ID]) []operation[ID] {
	ops := make([]operation[ID], 0, len(transactions))
	for _, tx := range transactions {
		ops = append(ops, newInitialOperation(tx.ClientID, tx.Payload))
	}
	return ops
}

func nextState[ID comparable](state state.State[ID], successfulOperations, failedOperations []operation[ID]) state.State[ID] {
	return state.NextState(toTransitions(successfulOperations), toTransitions(failedOperations))
}

func toTransitions[ID comparable](ops []operation[ID]) []state.Transition[ID] {
	trs := make([]state.Transition[ID], 0, len(ops))
	for _, op := range ops {
		trs = append(trs, op.toTransition())
	}
	return trs
}

func nextOperations[ID comparable](currentState state.State[ID], ops []operation[ID]) []operation[ID] {
	payloadByClientID, transitions := toTransitionsWithPayloads(ops)
	nextTrs := currentState.NextTransitions(transitions)
	return toOperations(nextTrs, payloadByClientID)
}

func toTransitionsWithPayloads[ID comparable](ops []operation[ID]) (map[ID]client.PreparePayload, []state.Transition[ID]) {
	payloadByClientID := make(map[ID]client.PreparePayload)
	transitions := make([]state.Transition[ID], 0, len(ops))
	for _, op := range ops {
		transitions = append(transitions, op.toTransition())
		payloadByClientID[op.clientID] = op.payload
	}
	return payloadByClientID, transitions
}

func toOperations[ID comparable](
	nextTransitions []state.Transition[ID],
	payloadByClientID map[ID]client.PreparePayload,
) []operation[ID] {
	nextOps := make([]operation[ID], 0, len(nextTransitions))
	for _, nextTr := range nextTransitions {
		nextOp := operation[ID]{
			clientID:    nextTr.GetClientID(),
			payload:     payloadByClientID[nextTr.GetClientID()],
			sourceState: nextTr.GetSourceState(),
			targetState: nextTr.GetTargetState(),
		}
		nextOps = append(nextOps, nextOp)
	}
	return nextOps
}

func (oh Coordinator[ID]) runOperationsConcurrently(
	ctx context.Context,
	resultCh chan<- operationResult[ID],
	transactionID string,
	operations []operation[ID],
) {
	var wg sync.WaitGroup

	for _, op := range operations {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := oh.runOperation(ctx, transactionID, op)
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

func (oh Coordinator[ID]) runOperation(ctx context.Context, transactionID string, operation operation[ID]) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	operationSentCh := oh.sendOperation(ctx, transactionID, operation)

	ctx, persistCancel := context.WithTimeout(ctx, persistStateTimeout)
	defer persistCancel()
	persistResultCh := oh.statePersister.PersistState(ctx, transactionID, operation.clientID, operation.targetState)

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

func (oh Coordinator[ID]) sendOperation(ctx context.Context, transactionID string, operation operation[ID]) <-chan error {
	operationDoneCh := make(chan error)

	go func() {
		operationDoneCh <- oh._sendOperation(ctx, transactionID, operation)
	}()

	return operationDoneCh
}

func (oh Coordinator[ID]) _sendOperation(ctx context.Context, transactionID string, operation operation[ID]) error {
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

type operation[ID comparable] struct {
	clientID    ID
	payload     client.PreparePayload
	sourceState transaction.State
	targetState transaction.State
}

func newInitialOperation[ID comparable](clientID ID, payload client.PreparePayload) operation[ID] {
	return operation[ID]{
		clientID:    clientID,
		payload:     payload,
		sourceState: transaction.NotStarted,
		targetState: transaction.Prepared,
	}
}

func (o operation[ID]) toTransition() state.Transition[ID] {
	return state.NewTransition(o.clientID, o.sourceState, o.targetState)
}
