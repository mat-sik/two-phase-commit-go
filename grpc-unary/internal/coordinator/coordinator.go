package coordinator

import (
	"context"
	"errors"
	"sync"
	"time"

	pb "github.com/mat-sik/two-phase-commit-go/grpc-unary/internal/generated/client/v1"
)

type OperationHandler struct {
	stateLoader     StateLoader
	statePersister  StatePersister
	clientRegistrar clientRegistrar
}

func NewOperationHandler(stateLoader StateLoader, statePersister StatePersister) *OperationHandler {
	return &OperationHandler{
		stateLoader:     stateLoader,
		statePersister:  statePersister,
		clientRegistrar: clientRegistrar{store: &clientRegistrarStore{}},
	}
}

type StatePersister interface {
	PersistState(ctx context.Context, transactionID string, targetHost string, transactionState TransactionState) <-chan PersistResult
}

type PersistResult struct {
	Commit   func() error
	Rollback func() error
	Err      error
}

func (oh OperationHandler) HandleRequest(ctx context.Context, request AtomicTransactions) error {
	initialState := oh.stateLoader.loadState(request.TransactionID, request.Transactions)

	var allErrs []error
	var successfulTransitions []stateTransition
	var failedTransitions []stateTransition
	for currState := initialState; !currState.allFinished(len(request.Transactions)); currState = currState.nextState(successfulTransitions, failedTransitions) {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(allErrs, err)...)
		}

		transitions := currState.nextStateTransitions(request.Transactions)

		resultCh := make(chan operationResult, len(transitions))
		oh.doTransitionsConcurrently(ctx, resultCh, request.TransactionID, transitions)

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

func (oh OperationHandler) doTransitionsConcurrently(
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
		return prepareOperation{trgHost: tr.host(), payload: tr.transaction.Payload}
	case commitStateTransition:
		return commitOperation{trgHost: tr.host()}
	case rollbackStateTransition:
		return commitOperation{trgHost: tr.host()}
	default:
		panic("unknown transition type")
	}
}

type AtomicTransactions struct {
	TransactionID string
	Transactions  []Transaction
}

type Transaction struct {
	TargetHost string
	Payload    string
}

func (oh OperationHandler) runOperation(ctx context.Context, transactionID string, operation operation) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	operationSentCh := oh.sendOperation(ctx, transactionID, operation)

	ctx, persistCancel := context.WithTimeout(ctx, persistStateTimeout)
	defer persistCancel()
	persistResultCh := oh.statePersister.PersistState(ctx, transactionID, operation.targetHost(), operation.postOperationTransactionState())

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

func (oh OperationHandler) sendOperation(ctx context.Context, transactionID string, operation operation) <-chan error {
	operationDoneCh := make(chan error)

	go func() {
		operationDoneCh <- oh._sendOperation(ctx, transactionID, operation)
	}()

	return operationDoneCh
}

func (oh OperationHandler) _sendOperation(ctx context.Context, transactionID string, operation operation) error {
	ctx, cancel := context.WithTimeout(ctx, sendOperationTimeout)
	defer cancel()

	client, err := oh.clientRegistrar.getClient(operation.targetHost())
	if err != nil {
		return err
	}
	switch op := operation.(type) {
	case prepareOperation:
		return handlePrepareOperation(ctx, client, transactionID, op)
	case commitOperation:
		return handleCommitOperation(ctx, client, transactionID)
	case rollbackOperation:
		return handleRollbackOperation(ctx, client, transactionID)
	default:
		panic(errors.New("unknown operation type"))
	}
}

const sendOperationTimeout = 5 * time.Second

func handlePrepareOperation(ctx context.Context, client pb.ClientServiceClient, transactionID string, operation prepareOperation) error {
	req := pb.PrepareTransactionRequest{TransactionId: transactionID, Payload: operation.payload}
	_, err := client.PrepareTransaction(ctx, &req)
	return err
}

func handleCommitOperation(ctx context.Context, client pb.ClientServiceClient, transactionID string) error {
	req := pb.CommitTransactionRequest{TransactionId: transactionID}
	_, err := client.CommitTransaction(ctx, &req)
	return err
}

func handleRollbackOperation(ctx context.Context, client pb.ClientServiceClient, transactionID string) error {
	req := pb.RollbackTransactionRequest{TransactionId: transactionID}
	_, err := client.RollbackTransaction(ctx, &req)
	return err
}

type operation interface {
	targetHost() string
	postOperationTransactionState() TransactionState
}

type prepareOperation struct {
	trgHost string
	payload string
}

func (o prepareOperation) targetHost() string {
	return o.trgHost
}

func (o prepareOperation) postOperationTransactionState() TransactionState {
	return transactionPrepared
}

type commitOperation struct {
	trgHost string
}

func (o commitOperation) targetHost() string {
	return o.trgHost
}

func (o commitOperation) postOperationTransactionState() TransactionState {
	return transactionCommitted
}

type rollbackOperation struct {
	trgHost string
}

func (o rollbackOperation) targetHost() string {
	return o.trgHost
}

func (o rollbackOperation) postOperationTransactionState() TransactionState {
	return transactionRolledBack
}
