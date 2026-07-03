package persister

import (
	"context"

	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type MockTransactionStatePersister struct {
	Err error
}

func (m MockTransactionStatePersister) PersistState(_ context.Context, _ string, _ string, _ twopc.TransactionState) error {
	return m.Err
}

type MockTransactionStateChecker struct {
	stateByParticipantID map[string]twopc.TransactionState
	err                  error
}

func (m MockTransactionStateChecker) Check(_ context.Context, _ string) (map[string]twopc.TransactionState, error) {
	return m.stateByParticipantID, m.err
}
