package coordinator

import (
	"context"

	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type MockStatePersister struct {
	err error
}

func (m MockStatePersister) PersistState(_ context.Context, _ string, _ string, _ twopc.TransactionState) <-chan twopc.PersistResult {
	ch := make(chan twopc.PersistResult, 1)
	if m.err != nil {
		ch <- twopc.PersistResult{Err: m.err}
	} else {
		ch <- twopc.PersistResult{
			Commit:   func() error { return nil },
			Rollback: func() error { return nil },
		}
	}
	return ch
}

type MockTransactionStateChecker struct {
	stateByClientID map[string]twopc.TransactionState
	err             error
}

func (m MockTransactionStateChecker) Check(_ string) (map[string]twopc.TransactionState, error) {
	return m.stateByClientID, m.err
}
