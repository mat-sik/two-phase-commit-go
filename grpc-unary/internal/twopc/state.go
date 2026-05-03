package twopc

import (
	"errors"
	"fmt"
	"maps"
)

type state struct {
	stateSets stateSets
}

func (s state) nextState(successfulTransitions []stateTransition, failedTransitions []stateTransition) state {
	if len(successfulTransitions) == 0 && len(failedTransitions) == 0 {
		return s
	}

	sets := buildStateSets(s, successfulTransitions, failedTransitions)

	return state{
		stateSets: sets,
	}
}

func buildStateSets(s state, successfulTransitions []stateTransition, failedTransitions []stateTransition) stateSets {
	clonedStateSets := s.stateSets.clone()

	for _, tr := range successfulTransitions {
		sourceTransactionState := tr.sourceState()
		targetTransactionState := transactionStateAfterSuccessfulTransition(tr)
		clonedStateSets.deleteValueFromSet(sourceTransactionState, tr.ClientIdentifier())
		clonedStateSets.addValueToSet(targetTransactionState, tr.ClientIdentifier())
	}

	for _, tr := range failedTransitions {
		sourceTransactionState := tr.sourceState()
		targetTransactionState := transactionStateAfterFailedTransition(tr)
		clonedStateSets.deleteValueFromSet(sourceTransactionState, tr.ClientIdentifier())
		clonedStateSets.addValueToSet(targetTransactionState, tr.ClientIdentifier())
	}

	return clonedStateSets
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

	if s.stateSets.allFinished(len(transactions)) {
		return nil, nil
	}

	if s.stateSets.anyPreparedFailed() {
		return s.buildRollbackStateTransitions(transactions), nil
	}

	if !s.stateSets.allPrepared(len(transactions)) && !s.stateSets.anyCommited() {
		return s.buildPrepareStateTransitions(transactions), nil
	}

	if !s.stateSets.allCommitted(len(transactions)) {
		return s.buildCommitStateTransitions(transactions), nil
	}

	panic(errors.New("should not be possible"))
}

func (s state) isInInvalidState() error {
	if s.stateSets.anyCommited() && s.stateSets.anyRolledBack() {
		return invalidStateErr(s.stateSets)
	}
	if s.stateSets.anyPreparedFailed() && s.stateSets.anyCommited() {
		return invalidStateErr(s.stateSets)
	}
	return nil
}

func invalidStateErr(sets stateSets) error {
	return fmt.Errorf("invalid state, prepared count: %d, prepareFailedCount: %d, commited count: %d, rolled back count: %d",
		len(sets.prepared), len(sets.prepareFailed), len(sets.committed), len(sets.rolledBack))
}

func (s state) buildPrepareStateTransitions(transactions []Transaction) []stateTransition {
	transitions := make([]stateTransition, 0, len(transactions)-s.stateSets.preparedAmount())
	for _, tx := range transactions {
		if !s.stateSets.prepared.has(tx.ClientIdentifier()) {
			txState := s.stateSets.transactionState(tx.ClientIdentifier())
			transitions = append(transitions, prepareStateTransition{preTransitionState: txState, transaction: tx})
		}
	}
	return transitions
}

func (s state) buildCommitStateTransitions(transactions []Transaction) []stateTransition {
	transitions := make([]stateTransition, 0, len(transactions)-s.stateSets.committedAmount())
	for _, tx := range transactions {
		if !s.stateSets.committed.has(tx.ClientIdentifier()) {
			txState := s.stateSets.transactionState(tx.ClientIdentifier())
			transitions = append(transitions, commitStateTransition{preTransitionState: txState, transaction: tx})
		}
	}
	return transitions
}

func (s state) buildRollbackStateTransitions(transactions []Transaction) []stateTransition {
	transitions := make([]stateTransition, 0, len(transactions)-s.stateSets.rolledBackAmount())
	for _, tx := range transactions {
		if !s.stateSets.rolledBack.has(tx.ClientIdentifier()) {
			txState := s.stateSets.transactionState(tx.ClientIdentifier())
			transitions = append(transitions, rollbackStateTransition{preTransitionState: txState, transaction: tx})
		}
	}
	return transitions
}

func (s state) isTerminalState(txAmount int) bool {
	return s.stateSets.allFinished(txAmount)
}

type stateSet map[ClientID]struct{}

func (s stateSet) add(clientID ClientID) {
	s[clientID] = struct{}{}
}

func (s stateSet) remove(clientID ClientID) {
	delete(s, clientID)
}

func (s stateSet) has(clientID ClientID) bool {
	_, ok := s[clientID]
	return ok
}

type stateSets struct {
	prepared      stateSet
	prepareFailed stateSet
	committed     stateSet
	rolledBack    stateSet
}

func (ss *stateSets) deleteValueFromSet(transactionState TransactionState, clientID ClientID) {
	set, ok := stateSetByTransactionState(*ss, transactionState)
	if !ok {
		return
	}
	set.remove(clientID)
}

func (ss *stateSets) addValueToSet(transactionState TransactionState, clientID ClientID) {
	set, ok := stateSetByTransactionState(*ss, transactionState)
	if !ok {
		return
	}
	set.add(clientID)
}

func stateSetByTransactionState(sets stateSets, transactionState TransactionState) (stateSet, bool) {
	var set stateSet
	switch transactionState {
	case transactionNotStarted:
		return nil, false
	case transactionPrepared:
		set = sets.prepared
	case transactionPrepareFailed:
		set = sets.prepareFailed
	case transactionCommitted:
		set = sets.committed
	case transactionRolledBack:
		set = sets.rolledBack
	}
	return set, true
}

func (ss *stateSets) transactionState(clientID ClientID) TransactionState {
	if ss.prepared.has(clientID) {
		return transactionPrepared
	}
	if ss.prepareFailed.has(clientID) {
		return transactionPrepareFailed
	}
	if ss.committed.has(clientID) {
		return transactionCommitted
	}
	if ss.rolledBack.has(clientID) {
		return transactionRolledBack
	}
	return transactionNotStarted
}

func (ss *stateSets) clone() stateSets {
	prepared := maps.Clone(ss.prepared)
	prepareFailed := maps.Clone(ss.prepareFailed)
	committed := maps.Clone(ss.committed)
	rolledBack := maps.Clone(ss.rolledBack)

	return stateSets{
		prepared:      prepared,
		prepareFailed: prepareFailed,
		committed:     committed,
		rolledBack:    rolledBack,
	}
}

func (ss *stateSets) allFinished(transactionsCount int) bool {
	return len(ss.committed) == transactionsCount || len(ss.rolledBack) == transactionsCount
}

func (ss *stateSets) allPrepared(transactionsCount int) bool {
	return len(ss.prepared) == transactionsCount
}

func (ss *stateSets) anyPreparedFailed() bool {
	return len(ss.prepareFailed) > 0
}

func (ss *stateSets) anyCommited() bool {
	return len(ss.committed) > 0
}

func (ss *stateSets) anyRolledBack() bool {
	return len(ss.rolledBack) > 0
}

func (ss *stateSets) allCommitted(transactionCount int) bool {
	return len(ss.committed) == transactionCount
}

func (ss *stateSets) preparedAmount() int {
	return len(ss.prepared)
}

func (ss *stateSets) committedAmount() int {
	return len(ss.prepared)
}

func (ss *stateSets) rolledBackAmount() int {
	return len(ss.prepared)
}

type stateTransition interface {
	ClientRegistrarUsable
	sourceState() TransactionState
}

type prepareStateTransition struct {
	preTransitionState TransactionState
	transaction        Transaction
}

func (tr prepareStateTransition) sourceState() TransactionState {
	return tr.preTransitionState
}

func (tr prepareStateTransition) ClientIdentifier() ClientID {
	return tr.transaction.ClientIdentifier()
}

type commitStateTransition struct {
	preTransitionState TransactionState
	transaction        Transaction
}

func (tr commitStateTransition) sourceState() TransactionState {
	return tr.preTransitionState
}

func (tr commitStateTransition) ClientIdentifier() ClientID {
	return tr.transaction.ClientIdentifier()
}

type rollbackStateTransition struct {
	preTransitionState TransactionState
	transaction        Transaction
}

func (tr rollbackStateTransition) sourceState() TransactionState {
	return tr.preTransitionState
}

func (tr rollbackStateTransition) ClientIdentifier() ClientID {
	return tr.transaction.ClientIdentifier()
}
