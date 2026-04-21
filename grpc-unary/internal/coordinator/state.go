package coordinator

import (
	"errors"
	"fmt"
	"maps"
)

type TransactionStateChecker interface {
	Check(transactionID string) map[string]TransactionState
}

type StateLoader struct {
	transactionStateChecker TransactionStateChecker
}

func NewStateLoader(transactionStateChecker TransactionStateChecker) StateLoader {
	return StateLoader{
		transactionStateChecker: transactionStateChecker,
	}
}

func (sl StateLoader) loadState(transactionID string, transactions []Transaction) state {
	prepared := make(map[string]struct{})
	prepareFailed := make(map[string]struct{})
	committed := make(map[string]struct{})
	rolledBack := make(map[string]struct{})

	stateByTargetHost := sl.transactionStateChecker.Check(transactionID)
	for _, op := range transactions {
		switch stateByTargetHost[op.TargetHost] {
		case transactionNotStarted:
			break
		case transactionPrepared:
			prepared[op.TargetHost] = struct{}{}
		case transactionPrepareFailed:
			prepareFailed[op.TargetHost] = struct{}{}
		case transactionCommitted:
			committed[op.TargetHost] = struct{}{}
		case transactionRolledBack:
			rolledBack[op.TargetHost] = struct{}{}
		}
	}

	return state{
		prepared:      prepared,
		prepareFailed: prepareFailed,
		committed:     committed,
		rolledBack:    rolledBack,
	}
}

type state struct {
	prepared      map[string]struct{}
	prepareFailed map[string]struct{}
	committed     map[string]struct{}
	rolledBack    map[string]struct{}
}

func (s state) nextState(successfulTransitions []stateTransition, failedTransitions []stateTransition) state {
	if len(successfulTransitions) == 0 && len(failedTransitions) == 0 {
		return s
	}

	prepared := maps.Clone(s.prepared)
	prepareFailed := maps.Clone(s.prepareFailed)
	committed := maps.Clone(s.committed)
	rolledBack := maps.Clone(s.rolledBack)

	for _, tr := range successfulTransitions {
		sourceTransactionState := tr.sourceState()
		targetTransactionState := transactionStateAfterSuccessfulTransition(tr)
		deleteValueFromMap(prepared, prepareFailed, committed, rolledBack, sourceTransactionState, tr.host())
		addValueToMap(prepared, prepareFailed, committed, rolledBack, targetTransactionState, tr.host())
	}

	for _, tr := range failedTransitions {
		sourceTransactionState := tr.sourceState()
		targetTransactionState := transactionStateAfterFailedTransition(tr)
		deleteValueFromMap(prepared, prepareFailed, committed, rolledBack, sourceTransactionState, tr.host())
		addValueToMap(prepared, prepareFailed, committed, rolledBack, targetTransactionState, tr.host())
	}

	return state{
		prepared:      prepared,
		prepareFailed: prepareFailed,
		committed:     committed,
		rolledBack:    rolledBack,
	}
}

func transactionStateAfterSuccessfulTransition(transition stateTransition) TransactionState {
	success := true
	return transactionStateAfterTransition(transition, success)
}

func transactionStateAfterFailedTransition(transition stateTransition) TransactionState {
	success := false
	return transactionStateAfterTransition(transition, success)
}

func transactionStateAfterTransition(transition stateTransition, success bool) TransactionState {
	switch transition.(type) {
	case prepareStateTransition:
		if success {
			return transactionPrepared
		}
		return transactionPrepareFailed
	case commitStateTransition:
		if success {
			return transactionCommitted
		}
		return transactionPrepared
	case rollbackStateTransition:
		if success {
			return transactionRolledBack
		}
		return transactionPrepareFailed
	default:
		panic(errors.New("unknown operation type"))
	}
}

func (s state) nextStateTransitions(transactions []Transaction) []stateTransition {
	transitions, err := s.tryNextStateTransitions(transactions)
	if err != nil {
		panic(err)
	}
	return transitions
}

func (s state) tryNextStateTransitions(transactions []Transaction) ([]stateTransition, error) {
	if err := s.isInInvalidState(); err != nil {
		return nil, err
	}

	if s.allFinished(len(transactions)) {
		return nil, nil
	}

	if s.anyPreparedFailed() {
		return s.buildRollbackStateTransitions(transactions), nil
	}

	if !s.allPrepared(len(transactions)) && !s.anyCommited() {
		return s.buildPrepareStateTransitions(transactions), nil
	}

	if !s.allCommitted(len(transactions)) {
		return s.buildCommitStateTransitions(transactions), nil
	}

	panic(errors.New("should not be possible"))
}

func (s state) isInInvalidState() error {
	if len(s.committed) > 0 && len(s.rolledBack) > 0 {
		return invalidStateErr(len(s.prepared), len(s.prepareFailed), len(s.committed), len(s.rolledBack))
	}
	if len(s.prepareFailed) > 0 && len(s.committed) > 0 {
		return invalidStateErr(len(s.prepared), len(s.prepareFailed), len(s.committed), len(s.rolledBack))
	}
	return nil
}

func invalidStateErr(preparedCount, prepareFailedCount, commitedCount, rolledBackCount int) error {
	return fmt.Errorf("invalid state, prepared count: %d, prepareFailedCount: %d, commited count: %d, rolled back count: %d",
		preparedCount, prepareFailedCount, commitedCount, rolledBackCount)
}

func (s state) allFinished(transactionsCount int) bool {
	return len(s.committed) == transactionsCount || len(s.rolledBack) == transactionsCount
}

func (s state) allPrepared(transactionsCount int) bool {
	return len(s.prepared) == transactionsCount
}

func (s state) anyPreparedFailed() bool {
	return len(s.prepareFailed) > 0
}

func (s state) anyCommited() bool {
	return len(s.committed) > 0
}

func (s state) allCommitted(transactionCount int) bool {
	return len(s.committed) == transactionCount
}

func (s state) buildPrepareStateTransitions(transactions []Transaction) []stateTransition {
	transitions := make([]stateTransition, 0, len(transactions)-len(s.prepared))
	for _, tr := range transactions {
		_, ok := s.prepared[tr.TargetHost]
		if !ok {
			transitions = append(transitions, prepareStateTransition{preTransitionState: s.transactionState(tr.TargetHost), transaction: tr})
		}
	}
	return transitions
}

func (s state) buildCommitStateTransitions(transactions []Transaction) []stateTransition {
	transitions := make([]stateTransition, 0, len(transactions)-len(s.committed))
	for _, tx := range transactions {
		_, ok := s.committed[tx.TargetHost]
		if !ok {
			transitions = append(transitions, commitStateTransition{preTransitionState: s.transactionState(tx.TargetHost), transaction: tx})
		}
	}
	return transitions
}

func (s state) buildRollbackStateTransitions(transactions []Transaction) []stateTransition {
	transitions := make([]stateTransition, 0, len(transactions)-len(s.rolledBack))
	for _, tr := range transactions {
		_, ok := s.rolledBack[tr.TargetHost]
		if !ok {
			transitions = append(transitions, rollbackStateTransition{preTransitionState: s.transactionState(tr.TargetHost), transaction: tr})
		}
	}
	return transitions
}

type stateTransition interface {
	sourceState() TransactionState
	host() string
}

type prepareStateTransition struct {
	preTransitionState TransactionState
	transaction        Transaction
}

func (tr prepareStateTransition) sourceState() TransactionState {
	return tr.preTransitionState
}

func (tr prepareStateTransition) host() string {
	return tr.transaction.TargetHost
}

type commitStateTransition struct {
	preTransitionState TransactionState
	transaction        Transaction
}

func (tr commitStateTransition) sourceState() TransactionState {
	return tr.preTransitionState
}

func (tr commitStateTransition) host() string {
	return tr.transaction.TargetHost
}

type rollbackStateTransition struct {
	preTransitionState TransactionState
	transaction        Transaction
}

func (tr rollbackStateTransition) sourceState() TransactionState {
	return tr.preTransitionState
}

func (tr rollbackStateTransition) host() string {
	return tr.transaction.TargetHost
}

func (s state) transactionState(targetHost string) TransactionState {
	if _, ok := s.prepared[targetHost]; ok {
		return transactionPrepared
	}
	if _, ok := s.prepareFailed[targetHost]; ok {
		return transactionPrepareFailed
	}
	if _, ok := s.committed[targetHost]; ok {
		return transactionCommitted
	}
	if _, ok := s.rolledBack[targetHost]; ok {
		return transactionRolledBack
	}
	return transactionNotStarted
}

func deleteValueFromMap(
	prepared map[string]struct{},
	prepareFailed map[string]struct{},
	committed map[string]struct{},
	rolledBack map[string]struct{},
	transactionState TransactionState,
	targetHost string,
) {
	switch transactionState {
	case transactionNotStarted:
		break
	case transactionPrepared:
		delete(prepared, targetHost)
	case transactionPrepareFailed:
		delete(prepareFailed, targetHost)
	case transactionCommitted:
		delete(committed, targetHost)
	case transactionRolledBack:
		delete(rolledBack, targetHost)
	}
}

func addValueToMap(
	prepared map[string]struct{},
	prepareFailed map[string]struct{},
	committed map[string]struct{},
	rolledBack map[string]struct{},
	transactionState TransactionState,
	targetHost string,
) {
	switch transactionState {
	case transactionNotStarted:
		break
	case transactionPrepared:
		prepared[targetHost] = struct{}{}
	case transactionPrepareFailed:
		prepareFailed[targetHost] = struct{}{}
	case transactionCommitted:
		committed[targetHost] = struct{}{}
	case transactionRolledBack:
		rolledBack[targetHost] = struct{}{}
	}
}
