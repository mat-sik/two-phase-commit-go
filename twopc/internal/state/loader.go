package state

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

type TransactionStateChecker[ID comparable] interface {
	Check(ctx context.Context, transactionID string) (map[ID]transaction.State, error)
}

type Loader[ID comparable] struct {
	transactionStateChecker TransactionStateChecker[ID]
}

func NewLoader[ID comparable](transactionStateChecker TransactionStateChecker[ID]) Loader[ID] {
	return Loader[ID]{
		transactionStateChecker: transactionStateChecker,
	}
}

func (l Loader[ID]) LoadState(ctx context.Context, transactionID string, participantIDs []ID) (State[ID], error) {
	if len(participantIDs) == 0 {
		return State[ID]{}, errors.New("participantIDs cannot be empty")
	}

	sets := stateSets[ID]{
		prepared:      make(stateSet[ID]),
		prepareFailed: make(stateSet[ID]),
		committed:     make(stateSet[ID]),
		rolledBack:    make(stateSet[ID]),
	}

	stateByParticipantID, err := l.transactionStateChecker.Check(ctx, transactionID)
	if err != nil {
		return State[ID]{}, fmt.Errorf("checking tx %s states: %w", transactionID, err)
	}

	participantIDsSet := toSet(participantIDs)

	if len(stateByParticipantID) == 0 {
		return State[ID]{
			participantIDs: participantIDsSet,
			stateSets:      sets,
		}, nil
	}

	if err = validateParticipantIDs(stateByParticipantID, participantIDsSet); err != nil {
		return State[ID]{}, err
	}

	if notPersisted, terminalFound := l.loadPersisted(sets, participantIDsSet, stateByParticipantID); terminalFound {
		l.assumeNotPersistedArePrepared(sets, notPersisted)
	}

	return State[ID]{
		participantIDs: participantIDsSet,
		stateSets:      sets,
	}, nil
}

func (l Loader[ID]) loadPersisted(
	sets stateSets[ID],
	participantIDs map[ID]struct{},
	stateByParticipantID map[ID]transaction.State,
) (notPersisted []ID, terminalFound bool) {
	for participantID := range participantIDs {
		if state, ok := stateByParticipantID[participantID]; ok {
			sets.addValueToSet(state, participantID)
			if state == transaction.PrepareFailed || state == transaction.Committed || state == transaction.RolledBack {
				terminalFound = true
			}
		} else {
			notPersisted = append(notPersisted, participantID)
		}
	}
	return notPersisted, terminalFound
}

func (l Loader[ID]) assumeNotPersistedArePrepared(sets stateSets[ID], notPersisted []ID) {
	for _, participantID := range notPersisted {
		sets.addValueToSet(transaction.Prepared, participantID)
	}
}

func toSet[ID comparable](participantIDs []ID) map[ID]struct{} {
	participantIDsSet := make(map[ID]struct{}, len(participantIDs))
	for _, participantID := range participantIDs {
		participantIDsSet[participantID] = struct{}{}
	}
	return participantIDsSet
}

func validateParticipantIDs[ID comparable](persisted map[ID]transaction.State, input map[ID]struct{}) error {
	for persistedParticipant := range persisted {
		if _, ok := input[persistedParticipant]; !ok {
			return fmt.Errorf("persisted participant %v not found in the input %v",
				persistedParticipant,
				slices.Collect(maps.Keys(input)),
			)
		}
	}
	return nil
}
