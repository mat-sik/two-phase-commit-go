package state

import (
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

type TransactionStateChecker[ID comparable] interface {
	Check(transactionID string) map[ID]transaction.State
}

type Loader[ID comparable] struct {
	transactionStateChecker TransactionStateChecker[ID]
}

func NewLoader[ID comparable](transactionStateChecker TransactionStateChecker[ID]) Loader[ID] {
	return Loader[ID]{
		transactionStateChecker: transactionStateChecker,
	}
}

func (sl Loader[ID]) LoadState(transactionID string, clientIDS []ID) State[ID] {
	prepared := make(stateSet[ID])
	prepareFailed := make(stateSet[ID])
	committed := make(stateSet[ID])
	rolledBack := make(stateSet[ID])

	stateByClientID := sl.transactionStateChecker.Check(transactionID)
	for _, clientID := range clientIDS {
		switch stateByClientID[clientID] {
		case transaction.NotStarted:
			break
		case transaction.Prepared:
			prepared.add(clientID)
		case transaction.PrepareFailed:
			prepareFailed.add(clientID)
		case transaction.Committed:
			committed.add(clientID)
		case transaction.RolledBack:
			rolledBack.add(clientID)
		}
	}

	return State[ID]{
		stateSets: stateSets[ID]{
			prepared:      prepared,
			prepareFailed: prepareFailed,
			committed:     committed,
			rolledBack:    rolledBack,
		},
	}
}
