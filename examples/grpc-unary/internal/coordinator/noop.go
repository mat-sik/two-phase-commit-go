package coordinator

import (
	"context"

	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type MockStatePersister struct {
	Err         error
	ErrCommit   error
	ErrRollback error
}

func (m MockStatePersister) PersistState(_ context.Context, _ string, _ string, _ twopc.TransactionState) <-chan twopc.PersistResult {
	ch := make(chan twopc.PersistResult, 1)
	if m.Err != nil {
		ch <- twopc.PersistResult{Err: m.Err}
	} else {
		ch <- twopc.PersistResult{
			Commit:   func() error { return m.ErrCommit },
			Rollback: func() error { return m.ErrRollback },
		}
	}
	return ch
}

type MockTransactionStateChecker struct {
	stateByClientID map[string]twopc.TransactionState
	err             error
}

func (m MockTransactionStateChecker) Check(_ context.Context, _ string) (map[string]twopc.TransactionState, error) {
	return m.stateByClientID, m.err
}
