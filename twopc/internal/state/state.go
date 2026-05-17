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

func (s State[ID]) NextState(successful, failed []Transition[ID]) State[ID] {
	if len(successful) == 0 && len(failed) == 0 {
		return s
	}

	sets := buildStateSets(s, successful, failed)

	return State[ID]{
		stateSets: sets,
	}
}

func buildStateSets[ID comparable](s State[ID], successful, failed []Transition[ID]) stateSets[ID] {
	clonedStateSets := s.stateSets.clone()

	for _, tr := range successful {
		updateWithSuccessfulTransitions(clonedStateSets, tr)
	}

	for _, tr := range failed {
		updateWithFailedTransitions(clonedStateSets, tr)
	}

	return clonedStateSets
}

func updateWithSuccessfulTransitions[ID comparable](stateSets stateSets[ID], tr Transition[ID]) {
	stateSets.deleteValueFromSet(tr.sourceState, tr.participantID)
	stateSets.addValueToSet(tr.targetState, tr.participantID)
}

func updateWithFailedTransitions[ID comparable](stateSets stateSets[ID], tr Transition[ID]) {
	stateSets.deleteValueFromSet(tr.sourceState, tr.participantID)
	stateSets.addValueToSet(stateAfterFailure(tr.targetState), tr.participantID)
}

func stateAfterFailure(targetState transaction.State) transaction.State {
	switch targetState {
	case transaction.Prepared:
		return transaction.PrepareFailed
	case transaction.Committed:
		return transaction.Prepared
	case transaction.RolledBack:
		return transaction.PrepareFailed
	default:
		panic("unsupported target state")
	}
}

func (s State[ID]) NextTransitions(previousTransitions []Transition[ID]) []Transition[ID] {
	nextTransitions, err := s.tryNextTransitions(previousTransitions)
	if err != nil {
		panic(err)
	}
	return nextTransitions
}

func (s State[ID]) tryNextTransitions(prevTrs []Transition[ID]) ([]Transition[ID], error) {
	if err := s.isInInvalidState(); err != nil {
		return nil, err
	}
	if len(prevTrs) == 0 {
		return nil, errors.New("previous transitions cannot be empty")
	}

	if s.stateSets.allFinished(len(prevTrs)) {
		return nil, nil
	}

	if s.stateSets.anyPreparedFailed() {
		return s.nextRollbackTransitions(prevTrs), nil
	}

	if !s.stateSets.allPrepared(len(prevTrs)) && !s.stateSets.anyCommitted() {
		return s.nextPrepareTransitions(prevTrs), nil
	}

	if !s.stateSets.allCommitted(len(prevTrs)) {
		return s.nextCommitTransitions(prevTrs), nil
	}

	panic("unreachable")
}

func (s State[ID]) isInInvalidState() error {
	if s.stateSets.anyCommitted() && s.stateSets.anyRolledBack() {
		return invalidStateErr(s.stateSets)
	}
	if s.stateSets.anyPreparedFailed() && s.stateSets.anyCommitted() {
		return invalidStateErr(s.stateSets)
	}
	return nil
}

func invalidStateErr[ID comparable](sets stateSets[ID]) error {
	return fmt.Errorf("invalid state, prepared count: %d, prepareFailedCount: %d, committed count: %d, rolled back count: %d",
		len(sets.prepared), len(sets.prepareFailed), len(sets.committed), len(sets.rolledBack))
}

func (s State[ID]) nextPrepareTransitions(trs []Transition[ID]) []Transition[ID] {
	return s.nextTransitions(trs, s.stateSets.prepared, PrepareTransition)
}

func (s State[ID]) nextCommitTransitions(trs []Transition[ID]) []Transition[ID] {
	return s.nextTransitions(trs, s.stateSets.committed, CommitTransition)
}

func (s State[ID]) nextTransitions(
	trs []Transition[ID],
	skipSet stateSet[ID],
	newTransitionFunc func(participantID ID) Transition[ID],
) []Transition[ID] {
	newTransitions := make([]Transition[ID], 0, len(trs)-len(skipSet))
	for _, tr := range trs {
		if !skipSet.has(tr.participantID) {
			newTransitions = append(newTransitions, newTransitionFunc(tr.participantID))
		}
	}
	return newTransitions
}

func (s State[ID]) nextRollbackTransitions(trs []Transition[ID]) []Transition[ID] {
	newTransitions := make([]Transition[ID], 0, len(trs)-s.stateSets.rolledBackAmount())
	for _, tr := range trs {
		if !s.stateSets.rolledBack.has(tr.participantID) {
			sourceState := s.stateSets.transactionState(tr.participantID)
			newTransitions = append(newTransitions, RollbackTransition(tr.participantID, sourceState))
		}
	}
	return newTransitions
}

func (ss *stateSets[ID]) transactionState(participantID ID) transaction.State {
	if ss.prepared.has(participantID) {
		return transaction.Prepared
	}
	if ss.prepareFailed.has(participantID) {
		return transaction.PrepareFailed
	}
	if ss.committed.has(participantID) {
		return transaction.Committed
	}
	if ss.rolledBack.has(participantID) {
		return transaction.RolledBack
	}
	return transaction.NotStarted
}

func (s State[ID]) IsTerminal(totalOperationsAmount int) bool {
	return s.stateSets.allFinished(totalOperationsAmount)
}

func (s State[ID]) IsCommitted(totalOperationsAmount int) bool {
	return s.stateSets.allCommitted(totalOperationsAmount)
}

func (s State[ID]) IsRolledBack(totalOperationsAmount int) bool {
	return s.stateSets.allRolledBack(totalOperationsAmount)
}

type stateSet[ID comparable] map[ID]struct{}

func (s stateSet[ID]) add(participantID ID) {
	s[participantID] = struct{}{}
}

func (s stateSet[ID]) remove(participantID ID) {
	delete(s, participantID)
}

func (s stateSet[ID]) has(participantID ID) bool {
	_, ok := s[participantID]
	return ok
}

type stateSets[ID comparable] struct {
	prepared      stateSet[ID]
	prepareFailed stateSet[ID]
	committed     stateSet[ID]
	rolledBack    stateSet[ID]
}

func (ss *stateSets[ID]) deleteValueFromSet(txState transaction.State, participantID ID) {
	set, ok := stateSetByTransactionState(*ss, txState)
	if !ok {
		return
	}
	set.remove(participantID)
}

func (ss *stateSets[ID]) addValueToSet(txState transaction.State, participantID ID) {
	set, ok := stateSetByTransactionState(*ss, txState)
	if !ok {
		return
	}
	set.add(participantID)
}

func stateSetByTransactionState[ID comparable](sets stateSets[ID], txState transaction.State) (stateSet[ID], bool) {
	var set stateSet[ID]
	switch txState {
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

func (ss *stateSets[ID]) allCommitted(transactionCount int) bool {
	return len(ss.committed) == transactionCount
}

func (ss *stateSets[ID]) allRolledBack(transactionsCount int) bool {
	return len(ss.rolledBack) == transactionsCount
}

func (ss *stateSets[ID]) anyPreparedFailed() bool {
	return len(ss.prepareFailed) > 0
}

func (ss *stateSets[ID]) allPrepared(transactionsCount int) bool {
	return len(ss.prepared) == transactionsCount
}

func (ss *stateSets[ID]) anyCommitted() bool {
	return len(ss.committed) > 0
}

func (ss *stateSets[ID]) anyRolledBack() bool {
	return len(ss.rolledBack) > 0
}

func (ss *stateSets[ID]) rolledBackAmount() int {
	return len(ss.rolledBack)
}

type Transition[ID comparable] struct {
	participantID ID
	sourceState   transaction.State
	targetState   transaction.State
}

func (t Transition[ID]) ParticipantID() ID {
	return t.participantID
}

func (t Transition[ID]) SourceState() transaction.State {
	return t.sourceState
}

func (t Transition[ID]) TargetState() transaction.State {
	return t.targetState
}

func PrepareTransition[ID comparable](participantID ID) Transition[ID] {
	return newTransition(participantID, transaction.NotStarted, transaction.Prepared)
}

func CommitTransition[ID comparable](participantID ID) Transition[ID] {
	return newTransition(participantID, transaction.Prepared, transaction.Committed)
}

func RollbackTransition[ID comparable](participantID ID, sourceState transaction.State) Transition[ID] {
	if sourceState != transaction.Prepared && sourceState != transaction.PrepareFailed {
		panic("unreachable")
	}
	return newTransition(participantID, sourceState, transaction.RolledBack)
}

func newTransition[ID comparable](participantID ID, sourceState transaction.State, targetState transaction.State) Transition[ID] {
	return Transition[ID]{
		participantID: participantID,
		sourceState:   sourceState,
		targetState:   targetState,
	}
}
