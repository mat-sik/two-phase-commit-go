package persister

import (
	"context"

	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type MockTransactionStatePersister struct {
	Err         error
	ErrCommit   error
	ErrRollback error
}

func (m MockTransactionStatePersister) PersistState(_ context.Context, _ string, _ string, _ twopc.TransactionState) twopc.PersistResult {
	if m.Err != nil {
		return twopc.PersistResult{Err: m.Err}
	}
	return twopc.PersistResult{
		Commit:   func() error { return m.ErrCommit },
		Rollback: func() error { return m.ErrRollback },
	}
}

type MockTransactionStateChecker struct {
	stateByClientID map[string]twopc.TransactionState
	err             error
}

func (m MockTransactionStateChecker) Check(_ context.Context, _ string) (map[string]twopc.TransactionState, error) {
	return m.stateByClientID, m.err
}
