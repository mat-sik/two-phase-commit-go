package state

import (
	"context"
	"errors"
	"fmt"

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
		return State[ID]{}, err
	}

	if len(stateByParticipantID) == 0 {
		return State[ID]{
			participantIDs: toSet(participantIDs),
			stateSets:      sets,
		}, nil
	}

	if err = validateParticipantIDs(stateByParticipantID, participantIDs); err != nil {
		return State[ID]{}, err
	}

	for _, participantID := range participantIDs {
		sets.addValueToSet(stateByParticipantID[participantID], participantID)
	}

	return State[ID]{
		participantIDs: toSet(participantIDs),
		stateSets:      sets,
	}, nil
}

func toSet[ID comparable](participantIDs []ID) map[ID]struct{} {
	participantIDsSet := make(map[ID]struct{}, len(participantIDs))
	for _, participantID := range participantIDs {
		participantIDsSet[participantID] = struct{}{}
	}
	return participantIDsSet
}

func validateParticipantIDs[ID comparable](loadedFromPersistentStore map[ID]transaction.State, providedAsInput []ID) error {
	if len(loadedFromPersistentStore) != len(providedAsInput) {
		return errors.New("differing amount of participants")
	}
	for _, id := range providedAsInput {
		_, ok := loadedFromPersistentStore[id]
		if !ok {
			return fmt.Errorf("participant: %v not present in the loaded", id)
		}
	}
	return nil
}
