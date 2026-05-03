package twopc

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Coordinator struct {
	stateLoader     StateLoader
	statePersister  StatePersister
	clientRegistrar clientRegistrar
}

func NewCoordinator(
	stateLoader StateLoader,
	statePersister StatePersister,
	newClientFunc func(identifiable ClientRegistrarUsable) (Client, error),
) *Coordinator {
	return &Coordinator{
		stateLoader:     stateLoader,
		statePersister:  statePersister,
		clientRegistrar: newClientRegistrar(newClientFunc),
	}
}

type StatePersister interface {
	PersistState(ctx context.Context, transactionID string, clientID ClientID, transactionState TransactionState) <-chan PersistResult
}

type PersistResult struct {
	Commit   func() error
	Rollback func() error
	Err      error
}

func (oh Coordinator) Execute(
	ctx context.Context,
	distributedTransaction DistributedTransaction,
) error {
	initialState := oh.stateLoader.loadState(distributedTransaction.TransactionID, distributedTransaction.Transactions)

	var allErrs []error
	var successfulTransitions []stateTransition
	var failedTransitions []stateTransition
	for currState := initialState; !currState.isTerminalState(len(distributedTransaction.Transactions)); currState = currState.nextState(successfulTransitions, failedTransitions) {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(allErrs, err)...)
		}

		transitions := currState.nextStateTransitions(distributedTransaction.Transactions)

		resultCh := make(chan operationResult, len(transitions))
		oh.doTransitionsConcurrently(ctx, resultCh, distributedTransaction.TransactionID, transitions)

		successfulTransitions = successfulTransitions[:0]
		failedTransitions = failedTransitions[:0]
		var errs []error
		for result := range resultCh {
			if result.operationErr != nil {
				failedTransitions = append(failedTransitions, result.transition)
				errs = append(errs, result.operationErr)
			} else {
				successfulTransitions = append(successfulTransitions, result.transition)
			}
		}

		if len(errs) > 0 {
			allErrs = append(allErrs, errors.Join(errs...))
		}
	}
	return errors.Join(allErrs...)
}

func (oh Coordinator) doTransitionsConcurrently(
	ctx context.Context,
	resultCh chan<- operationResult,
	transactionID string,
	transitions []stateTransition,
) {
	var wg sync.WaitGroup

	for _, transition := range transitions {
		wg.Add(1)
		op := mapToOperation(transition)
		go func() {
			defer wg.Done()
			err := oh.runOperation(ctx, transactionID, op)
			resultCh <- operationResult{operationErr: err, transition: transition}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()
}

type operationResult struct {
	operationErr error
	transition   stateTransition
}

func mapToOperation(transition stateTransition) operation {
	switch tr := transition.(type) {
	case prepareStateTransition:
		return prepareOperation{clientID: tr.ClientIdentifier(), payload: tr.transaction.Payload}
	case commitStateTransition:
		return commitOperation{clientID: tr.ClientIdentifier()}
	case rollbackStateTransition:
		return rollbackOperation{clientID: tr.ClientIdentifier()}
	default:
		panic("unknown transition type")
	}
}

type DistributedTransaction struct {
	TransactionID string
	Transactions  []Transaction
}

type Transaction struct {
	ClientID ClientID
	Payload  string
}

func NewTransaction(clientIDString string, payload string) Transaction {
	return Transaction{
		ClientID: ClientID(clientIDString),
		Payload:  payload,
	}
}

func (t Transaction) ClientIdentifier() ClientID {
	return t.ClientID
}

func (oh Coordinator) runOperation(ctx context.Context, transactionID string, operation operation) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	operationSentCh := oh.sendOperation(ctx, transactionID, operation)

	ctx, persistCancel := context.WithTimeout(ctx, persistStateTimeout)
	defer persistCancel()
	persistResultCh := oh.statePersister.PersistState(ctx, transactionID, operation.ClientIdentifier(), operation.postOperationTransactionState())

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

	client, err := oh.clientRegistrar.getClient(operation)
	if err != nil {
		return err
	}
	switch op := operation.(type) {
	case prepareOperation:
		return client.prepareTransaction(ctx, transactionID, op)
	case commitOperation:
		return client.commitTransaction(ctx, transactionID)
	case rollbackOperation:
		return client.rollbackTransaction(ctx, transactionID)
	default:
		panic(errors.New("unknown operation type"))
	}
}

const sendOperationTimeout = 5 * time.Second

type operation interface {
	ClientRegistrarUsable
	postOperationTransactionState() TransactionState
}

type prepareOperation struct {
	clientID ClientID
	payload  string
}

func (o prepareOperation) ClientIdentifier() ClientID {
	return o.clientID
}

func (o prepareOperation) postOperationTransactionState() TransactionState {
	return transactionPrepared
}

type commitOperation struct {
	clientID ClientID
}

func (o commitOperation) ClientIdentifier() ClientID {
	return o.clientID
}

func (o commitOperation) postOperationTransactionState() TransactionState {
	return transactionCommitted
}

type rollbackOperation struct {
	clientID ClientID
}

func (o rollbackOperation) ClientIdentifier() ClientID {
	return o.clientID
}

func (o rollbackOperation) postOperationTransactionState() TransactionState {
	return transactionRolledBack
}
