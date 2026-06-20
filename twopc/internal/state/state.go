package state

import (
	"fmt"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

type State[ID comparable] struct {
	participantIDs map[ID]struct{}
	stateSets      stateSets[ID]
}

func (s State[ID]) NextState(successful, failed []Transition[ID]) {
	if len(successful) == 0 && len(failed) == 0 {
		return
	}

	s.stateSets.nextState(successful, failed)
}

func (s State[ID]) NextTransitions() []Transition[ID] {
	nextTransitions, err := s.tryNextTransitions()
	if err != nil {
		panic(err)
	}
	return nextTransitions
}

func (s State[ID]) tryNextTransitions() ([]Transition[ID], error) {
	if err := s.isInInvalidState(); err != nil {
		return nil, err
	}

	if s.IsTerminal() {
		return nil, nil
	}

	if s.shouldIssueRollbacks() {
		return s.nextRollbackTransitions(), nil
	}

	if s.shouldIssuePrepares() {
		return s.nextPrepareTransitions(), nil
	}

	if !s.IsCommitted() {
		return s.nextCommitTransitions(), nil
	}

	panic("logic should prohibit this")
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
	return fmt.Errorf("invalid state, prepared count: %d, prepare failed count: %d, committed count: %d, rolled back count: %d",
		len(sets.prepared), len(sets.prepareFailed), len(sets.committed), len(sets.rolledBack))
}

func (s State[ID]) shouldIssueRollbacks() bool {
	return s.stateSets.anyPreparedFailed() || s.stateSets.anyRolledBack()
}

func (s State[ID]) shouldIssuePrepares() bool {
	return !s.isPrepared() && !s.stateSets.anyCommitted()
}

func (s State[ID]) IsTerminal() bool {
	return s.IsCommitted() || s.IsRolledBack()
}

func (s State[ID]) isPrepared() bool {
	return s.stateSets.allPrepared(len(s.participantIDs))
}

func (s State[ID]) IsCommitted() bool {
	return s.stateSets.allCommitted(len(s.participantIDs))
}

func (s State[ID]) IsRolledBack() bool {
	return s.stateSets.allRolledBack(len(s.participantIDs))
}

func (s State[ID]) nextPrepareTransitions() []Transition[ID] {
	return s.nextTransitions(s.stateSets.prepared, PrepareTransition)
}

func (s State[ID]) nextCommitTransitions() []Transition[ID] {
	return s.nextTransitions(s.stateSets.committed, CommitTransition)
}

func (s State[ID]) nextTransitions(skipSet stateSet[ID],
	newTransitionFunc func(participantID ID) Transition[ID],
) []Transition[ID] {
	newTransitions := make([]Transition[ID], 0, len(s.participantIDs)-len(skipSet))
	for participantID := range s.participantIDs {
		if !skipSet.has(participantID) {
			newTransitions = append(newTransitions, newTransitionFunc(participantID))
		}
	}
	return newTransitions
}

func (s State[ID]) nextRollbackTransitions() []Transition[ID] {
	newTransitions := make([]Transition[ID], 0, len(s.participantIDs)-s.stateSets.rolledBackCount())
	for participantID := range s.participantIDs {
		if !s.stateSets.rolledBack.has(participantID) {
			sourceState := s.stateSets.transactionState(participantID)
			newTransitions = append(newTransitions, RollbackTransition(participantID, sourceState))
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

func (ss *stateSets[ID]) nextState(successful, failed []Transition[ID]) {
	for _, tr := range successful {
		ss.updateWithSuccessfulTransitions(tr)
	}
	for _, tr := range failed {
		ss.updateWithFailedTransitions(tr)
	}
}

func (ss *stateSets[ID]) updateWithSuccessfulTransitions(tr Transition[ID]) {
	ss.deleteValueFromSet(tr.sourceState, tr.participantID)
	ss.addValueToSet(tr.targetState, tr.participantID)
}

func (ss *stateSets[ID]) updateWithFailedTransitions(tr Transition[ID]) {
	ss.deleteValueFromSet(tr.sourceState, tr.participantID)
	ss.addValueToSet(stateAfterFailure(tr.targetState), tr.participantID)
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

func (ss *stateSets[ID]) deleteValueFromSet(txState transaction.State, participantID ID) {
	set, ok := ss.stateSetByTransactionState(txState)
	if !ok {
		return
	}
	set.remove(participantID)
}

func (ss *stateSets[ID]) addValueToSet(txState transaction.State, participantID ID) {
	set, ok := ss.stateSetByTransactionState(txState)
	if !ok {
		return
	}
	set.add(participantID)
}

func (ss *stateSets[ID]) stateSetByTransactionState(txState transaction.State) (stateSet[ID], bool) {
	var set stateSet[ID]
	switch txState {
	case transaction.NotStarted:
		return nil, false
	case transaction.Prepared:
		set = ss.prepared
	case transaction.PrepareFailed:
		set = ss.prepareFailed
	case transaction.Committed:
		set = ss.committed
	case transaction.RolledBack:
		set = ss.rolledBack
	}
	return set, true
}

func (ss *stateSets[ID]) allCommitted(participantCount int) bool {
	return len(ss.committed) == participantCount
}

func (ss *stateSets[ID]) allRolledBack(participantCount int) bool {
	return len(ss.rolledBack) == participantCount
}

func (ss *stateSets[ID]) anyPreparedFailed() bool {
	return len(ss.prepareFailed) > 0
}

func (ss *stateSets[ID]) allPrepared(participantCount int) bool {
	return len(ss.prepared) == participantCount
}

func (ss *stateSets[ID]) anyCommitted() bool {
	return len(ss.committed) > 0
}

func (ss *stateSets[ID]) anyRolledBack() bool {
	return len(ss.rolledBack) > 0
}

func (ss *stateSets[ID]) rolledBackCount() int {
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
		panic("logic should prohibit this")
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
