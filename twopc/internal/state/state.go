package state

import (
	"errors"
	"fmt"
	"maps"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/client"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

type State struct {
	stateSets stateSets
}

func (s State) NextState(successfulTransitions, failedTransitions []Transition) State {
	if len(successfulTransitions) == 0 && len(failedTransitions) == 0 {
		return s
	}

	sets := buildStateSets(s, successfulTransitions, failedTransitions)

	return State{
		stateSets: sets,
	}
}

func buildStateSets(s State, successfulTransitions, failedTransitions []Transition) stateSets {
	clonedStateSets := s.stateSets.clone()

	for _, tr := range successfulTransitions {
		updateWithTransitions(clonedStateSets, tr, currentStateAfterSuccessfulTransition)
	}

	for _, tr := range failedTransitions {
		updateWithTransitions(clonedStateSets, tr, currentStateAfterFailedTransition)
	}

	return clonedStateSets
}

func updateWithTransitions(
	stateSets stateSets,
	tr Transition,
	currentStateFunc func(targetState transaction.State) transaction.State,
) {
	previousState := tr.sourceState
	currentState := currentStateFunc(tr.targetState)
	stateSets.deleteValueFromSet(previousState, tr.clientID)
	stateSets.addValueToSet(currentState, tr.clientID)
}

func currentStateAfterSuccessfulTransition(targetState transaction.State) transaction.State {
	success := true
	return transactionStateAfterTransition(targetState, success)
}

func currentStateAfterFailedTransition(targetState transaction.State) transaction.State {
	success := false
	return transactionStateAfterTransition(targetState, success)
}

func transactionStateAfterTransition(targetState transaction.State, success bool) transaction.State {
	switch targetState {
	case transaction.Prepared:
		if success {
			return transaction.Prepared
		}
		return transaction.PrepareFailed
	case transaction.Committed:
		if success {
			return transaction.Committed
		}
		return transaction.Prepared
	case transaction.RolledBack:
		if success {
			return transaction.RolledBack
		}
		return transaction.PrepareFailed
	default:
		panic(errors.New("unsupported target state"))
	}
}

func (s State) NextTransitions(transitions []Transition) []Transition {
	transitions, err := s.tryNextTransitions(transitions)
	if err != nil {
		panic(err)
	}
	return transitions
}

func (s State) tryNextTransitions(transitions []Transition) ([]Transition, error) {
	if err := s.isInInvalidState(); err != nil {
		return nil, err
	}

	if s.stateSets.allFinished(len(transitions)) {
		return nil, nil
	}

	if s.stateSets.anyPreparedFailed() {
		return s.nextRollbackTransitions(transitions), nil
	}

	if !s.stateSets.allPrepared(len(transitions)) && !s.stateSets.anyCommited() {
		return s.nextPrepareTransitions(transitions), nil
	}

	if !s.stateSets.allCommitted(len(transitions)) {
		return s.nextCommitTransitions(transitions), nil
	}

	panic(errors.New("should not be possible"))
}

func (s State) isInInvalidState() error {
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

func (s State) nextPrepareTransitions(transitions []Transition) []Transition {
	return s.nextTransitions(transitions, s.stateSets.prepared, prepareTransition)
}

func (s State) nextCommitTransitions(transitions []Transition) []Transition {
	return s.nextTransitions(transitions, s.stateSets.committed, commitTransition)
}

func (s State) nextRollbackTransitions(transitions []Transition) []Transition {
	return s.nextTransitions(transitions, s.stateSets.rolledBack, rollbackTransition)
}

func (s State) nextTransitions(
	transitions []Transition,
	ignoreSet stateSet,
	newTransitionFunc func(clientID client.ID, transactionState transaction.State) Transition,
) []Transition {
	newTransitions := make([]Transition, 0, len(transitions)-s.stateSets.rolledBackAmount())
	for _, tr := range transitions {
		if !ignoreSet.has(tr.clientID) {
			sourceState := s.stateSets.transactionState(tr.clientID)
			newTransitions = append(newTransitions, newTransitionFunc(tr.clientID, sourceState))
		}
	}
	return newTransitions
}

func (s State) IsTerminalState(operationAmount int) bool {
	return s.stateSets.allFinished(operationAmount)
}

type stateSet map[client.ID]struct{}

func (s stateSet) add(clientID client.ID) {
	s[clientID] = struct{}{}
}

func (s stateSet) remove(clientID client.ID) {
	delete(s, clientID)
}

func (s stateSet) has(clientID client.ID) bool {
	_, ok := s[clientID]
	return ok
}

type stateSets struct {
	prepared      stateSet
	prepareFailed stateSet
	committed     stateSet
	rolledBack    stateSet
}

func (ss *stateSets) deleteValueFromSet(transactionState transaction.State, clientID client.ID) {
	set, ok := stateSetByTransactionState(*ss, transactionState)
	if !ok {
		return
	}
	set.remove(clientID)
}

func (ss *stateSets) addValueToSet(transactionState transaction.State, clientID client.ID) {
	set, ok := stateSetByTransactionState(*ss, transactionState)
	if !ok {
		return
	}
	set.add(clientID)
}

func stateSetByTransactionState(sets stateSets, transactionState transaction.State) (stateSet, bool) {
	var set stateSet
	switch transactionState {
	case transaction.NotStarted:
		return nil, false
	case transaction.Prepared:
		set = sets.prepared
	case transaction.PrepareFailed:
		set = sets.prepareFailed
	case transaction.Committed:
		set = sets.committed
	case transaction.RolledBack:
		set = sets.rolledBack
	}
	return set, true
}

func (ss *stateSets) transactionState(clientID client.ID) transaction.State {
	if ss.prepared.has(clientID) {
		return transaction.Prepared
	}
	if ss.prepareFailed.has(clientID) {
		return transaction.PrepareFailed
	}
	if ss.committed.has(clientID) {
		return transaction.Committed
	}
	if ss.rolledBack.has(clientID) {
		return transaction.RolledBack
	}
	return transaction.NotStarted
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

type Transition struct {
	clientID    client.ID
	sourceState transaction.State
	targetState transaction.State
}

func (t Transition) GetClientID() client.ID {
	return t.clientID
}

func (t Transition) GetSourceState() transaction.State {
	return t.sourceState
}

func (t Transition) GetTargetState() transaction.State {
	return t.targetState
}

func prepareTransition(clientID client.ID, sourceState transaction.State) Transition {
	return NewTransition(clientID, sourceState, transaction.Prepared)
}

func commitTransition(clientID client.ID, sourceState transaction.State) Transition {
	return NewTransition(clientID, sourceState, transaction.Committed)
}

func rollbackTransition(clientID client.ID, sourceState transaction.State) Transition {
	return NewTransition(clientID, sourceState, transaction.RolledBack)
}

func NewTransition(clientID client.ID, sourceState transaction.State, targetState transaction.State) Transition {
	return Transition{
		clientID:    clientID,
		sourceState: sourceState,
		targetState: targetState,
	}
}
