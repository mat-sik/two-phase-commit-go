package state

import (
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

type TransactionStateChecker[ID comparable] interface {
	Check(transactionID string) (map[ID]transaction.State, error)
}

type Loader[ID comparable] struct {
	transactionStateChecker TransactionStateChecker[ID]
}

func NewLoader[ID comparable](transactionStateChecker TransactionStateChecker[ID]) Loader[ID] {
	return Loader[ID]{
		transactionStateChecker: transactionStateChecker,
	}
}

func (sl Loader[ID]) LoadState(transactionID string, clientIDs []ID) (State[ID], error) {
	sets := stateSets[ID]{
		prepared:      make(stateSet[ID]),
		prepareFailed: make(stateSet[ID]),
		committed:     make(stateSet[ID]),
		rolledBack:    make(stateSet[ID]),
	}

	stateByClientID, err := sl.transactionStateChecker.Check(transactionID)
	if err != nil {
		return State[ID]{}, err
	}

	for _, clientID := range clientIDs {
		sets.addValueToSet(stateByClientID[clientID], clientID)
	}
	return State[ID]{stateSets: sets}, nil
}
