package twopc

type TransactionStateChecker interface {
	Check(transactionID string) map[ClientID]TransactionState
}

type StateLoader struct {
	transactionStateChecker TransactionStateChecker
}

func NewStateLoader(transactionStateChecker TransactionStateChecker) StateLoader {
	return StateLoader{
		transactionStateChecker: transactionStateChecker,
	}
}

func (sl StateLoader) loadState(transactionID string, transactions []Transaction) state {
	prepared := make(stateSet)
	prepareFailed := make(stateSet)
	committed := make(stateSet)
	rolledBack := make(stateSet)

	stateByClientID := sl.transactionStateChecker.Check(transactionID)
	for _, op := range transactions {
		switch stateByClientID[op.ClientIdentifier()] {
		case transactionNotStarted:
			break
		case transactionPrepared:
			prepared.add(op.ClientIdentifier())
		case transactionPrepareFailed:
			prepareFailed.add(op.ClientIdentifier())
		case transactionCommitted:
			committed.add(op.ClientIdentifier())
		case transactionRolledBack:
			rolledBack.add(op.ClientIdentifier())
		}
	}

	return state{
		stateSets: stateSets{
			prepared:      prepared,
			prepareFailed: prepareFailed,
			committed:     committed,
			rolledBack:    rolledBack,
		},
	}
}
