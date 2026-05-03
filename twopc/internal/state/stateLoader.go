package state

import (
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/client"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

type TransactionStateChecker interface {
	Check(transactionID string) map[client.ID]transaction.State
}

type Loader struct {
	transactionStateChecker TransactionStateChecker
}

func NewLoader(transactionStateChecker TransactionStateChecker) Loader {
	return Loader{
		transactionStateChecker: transactionStateChecker,
	}
}

func (sl Loader) LoadState(transactionID string, clientIDS []client.ID) State {
	prepared := make(stateSet)
	prepareFailed := make(stateSet)
	committed := make(stateSet)
	rolledBack := make(stateSet)

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

	return State{
		stateSets: stateSets{
			prepared:      prepared,
			prepareFailed: prepareFailed,
			committed:     committed,
			rolledBack:    rolledBack,
		},
	}
}
