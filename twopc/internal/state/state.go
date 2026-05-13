package state

import (
	"errors"
	"fmt"
	"maps"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

type State[ID comparable] struct {
	stateSets stateSets[ID]
}

func (s State[ID]) NextState(successfulTransitions, failedTransitions []Transition[ID]) State[ID] {
	if len(successfulTransitions) == 0 && len(failedTransitions) == 0 {
		return s
	}

	sets := buildStateSets(s, successfulTransitions, failedTransitions)

	return State[ID]{
		stateSets: sets,
	}
}

func buildStateSets[ID comparable](s State[ID], successfulTransitions, failedTransitions []Transition[ID]) stateSets[ID] {
	clonedStateSets := s.stateSets.clone()

	for _, tr := range successfulTransitions {
		updateWithTransitions(clonedStateSets, tr, currentStateAfterSuccessfulTransition)
	}

	for _, tr := range failedTransitions {
		updateWithTransitions(clonedStateSets, tr, currentStateAfterFailedTransition)
	}

	return clonedStateSets
}

func updateWithTransitions[ID comparable](
	stateSets stateSets[ID],
	tr Transition[ID],
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

func (s State[ID]) NextTransitions(transitions []Transition[ID]) []Transition[ID] {
	transitions, err := s.tryNextTransitions(transitions)
	if err != nil {
		panic(err)
	}
	return transitions
}

func (s State[ID]) tryNextTransitions(transitions []Transition[ID]) ([]Transition[ID], error) {
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

func (s State[ID]) isInInvalidState() error {
	if s.stateSets.anyCommited() && s.stateSets.anyRolledBack() {
		return invalidStateErr(s.stateSets)
	}
	if s.stateSets.anyPreparedFailed() && s.stateSets.anyCommited() {
		return invalidStateErr(s.stateSets)
	}
	return nil
}

func invalidStateErr[ID comparable](sets stateSets[ID]) error {
	return fmt.Errorf("invalid state, prepared count: %d, prepareFailedCount: %d, commited count: %d, rolled back count: %d",
		len(sets.prepared), len(sets.prepareFailed), len(sets.committed), len(sets.rolledBack))
}

func (s State[ID]) nextPrepareTransitions(transitions []Transition[ID]) []Transition[ID] {
	return s.nextTransitions(transitions, s.stateSets.prepared, prepareTransition)
}

func (s State[ID]) nextCommitTransitions(transitions []Transition[ID]) []Transition[ID] {
	return s.nextTransitions(transitions, s.stateSets.committed, commitTransition)
}

func (s State[ID]) nextTransitions(
	transitions []Transition[ID],
	ignoreSet stateSet[ID],
	newTransitionFunc func(clientID ID) Transition[ID],
) []Transition[ID] {
	newTransitions := make([]Transition[ID], 0, len(transitions)-s.stateSets.rolledBackAmount())
	for _, tr := range transitions {
		if !ignoreSet.has(tr.clientID) {
			newTransitions = append(newTransitions, newTransitionFunc(tr.clientID))
		}
	}
	return newTransitions
}

func (s State[ID]) nextRollbackTransitions(transitions []Transition[ID]) []Transition[ID] {
	newTransitions := make([]Transition[ID], 0, len(transitions)-s.stateSets.rolledBackAmount())
	for _, tr := range transitions {
		if !s.stateSets.rolledBack.has(tr.clientID) {
			sourceState := s.stateSets.transactionState(tr.clientID)
			newTransitions = append(newTransitions, rollbackTransition(tr.clientID, sourceState))
		}
	}
	return newTransitions
}

func (ss *stateSets[ID]) transactionState(clientID ID) transaction.State {
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

func (s State[ID]) IsTerminal(operationAmount int) bool {
	return s.stateSets.allFinished(operationAmount)
}

func (s State[ID]) IsRolledBack(operationAmount int) bool {
	return s.stateSets.allRolledBack(operationAmount)
}

type stateSet[ID comparable] map[ID]struct{}

func (s stateSet[ID]) add(clientID ID) {
	s[clientID] = struct{}{}
}

func (s stateSet[ID]) remove(clientID ID) {
	delete(s, clientID)
}

func (s stateSet[ID]) has(clientID ID) bool {
	_, ok := s[clientID]
	return ok
}

type stateSets[ID comparable] struct {
	prepared      stateSet[ID]
	prepareFailed stateSet[ID]
	committed     stateSet[ID]
	rolledBack    stateSet[ID]
}

func (ss *stateSets[ID]) deleteValueFromSet(transactionState transaction.State, clientID ID) {
	set, ok := stateSetByTransactionState(*ss, transactionState)
	if !ok {
		return
	}
	set.remove(clientID)
}

func (ss *stateSets[ID]) addValueToSet(transactionState transaction.State, clientID ID) {
	set, ok := stateSetByTransactionState(*ss, transactionState)
	if !ok {
		return
	}
	set.add(clientID)
}

func stateSetByTransactionState[ID comparable](sets stateSets[ID], transactionState transaction.State) (stateSet[ID], bool) {
	var set stateSet[ID]
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

func (ss *stateSets[ID]) clone() stateSets[ID] {
	prepared := maps.Clone(ss.prepared)
	prepareFailed := maps.Clone(ss.prepareFailed)
	committed := maps.Clone(ss.committed)
	rolledBack := maps.Clone(ss.rolledBack)

	return stateSets[ID]{
		prepared:      prepared,
		prepareFailed: prepareFailed,
		committed:     committed,
		rolledBack:    rolledBack,
	}
}

func (ss *stateSets[ID]) allFinished(transactionsCount int) bool {
	return ss.allCommitted(transactionsCount) || ss.allRolledBack(transactionsCount)
}

func (ss *stateSets[ID]) allRolledBack(transactionsCount int) bool {
	return len(ss.rolledBack) == transactionsCount
}

func (ss *stateSets[ID]) allPrepared(transactionsCount int) bool {
	return len(ss.prepared) == transactionsCount
}

func (ss *stateSets[ID]) anyPreparedFailed() bool {
	return len(ss.prepareFailed) > 0
}

func (ss *stateSets[ID]) anyCommited() bool {
	return len(ss.committed) > 0
}

func (ss *stateSets[ID]) anyRolledBack() bool {
	return len(ss.rolledBack) > 0
}

func (ss *stateSets[ID]) allCommitted(transactionCount int) bool {
	return len(ss.committed) == transactionCount
}

func (ss *stateSets[ID]) preparedAmount() int {
	return len(ss.prepared)
}

func (ss *stateSets[ID]) committedAmount() int {
	return len(ss.prepared)
}

func (ss *stateSets[ID]) rolledBackAmount() int {
	return len(ss.prepared)
}

type Transition[ID comparable] struct {
	clientID    ID
	sourceState transaction.State
	targetState transaction.State
}

func (t Transition[ID]) GetClientID() ID {
	return t.clientID
}

func (t Transition[ID]) GetSourceState() transaction.State {
	return t.sourceState
}

func (t Transition[ID]) GetTargetState() transaction.State {
	return t.targetState
}

func prepareTransition[ID comparable](clientID ID) Transition[ID] {
	return NewTransition(clientID, transaction.NotStarted, transaction.Prepared)
}

func commitTransition[ID comparable](clientID ID) Transition[ID] {
	return NewTransition(clientID, transaction.Prepared, transaction.Committed)
}

func rollbackTransition[ID comparable](clientID ID, sourceState transaction.State) Transition[ID] {
	return NewTransition(clientID, sourceState, transaction.RolledBack)
}

func NewTransition[ID comparable](clientID ID, sourceState transaction.State, targetState transaction.State) Transition[ID] {
	return Transition[ID]{
		clientID:    clientID,
		sourceState: sourceState,
		targetState: targetState,
	}
}
