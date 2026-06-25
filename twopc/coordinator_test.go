package twopc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TODO: These should be updated to be in sync with conclusions from state_test and lodaer_test
func TestCoordinator_Execute(t *testing.T) {
	tests := []compactedTestCase{
		// ═════════════════════════════════════════════════════════════════════════════
		// GROUP 0 - no coordinator failures
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name:        "a: NS -> P -> C",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
			},
		},
		{
			name: "a: NS -> PF",
			// TODO: Maybe this outcome should be renamed so that we can distinguish these two terminal states
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepare(),
				},
			},
		},
		{
			name:        "a: NS -> P -> C, b: NS -> P -> C",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionNotStarted,
				},
			},
		},
		{
			name:        "a: NS -> PF, b: NS -> PF",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepare(),
				},
				{
					id:     "host-b",
					state:  TransactionNotStarted,
					client: failPrepare(),
				},
			},
		},
		{
			name:        "a: NS -> P -> R, b: NS -> PF",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:     "host-b",
					state:  TransactionNotStarted,
					client: failPrepare(),
				},
			},
		},
		// ═════════════════════════════════════════════════════════════════════════════
		// GROUP 1 - retry from coordinator failure
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name:        "host-a: not started -> prepared -> committed, host-b: prepared -> committed",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepared,
				},
			},
		},
		{
			name:        "host-a: not started -> prepared -> committed, host-b: committed -> committed",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionCommitted,
				},
			},
		},
		{
			name:        "host-a: not started -> not started, host-b: prepare failed -> rolled back",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepareFailed,
				},
			},
		},
		{
			name:        "host-a: not started -> not started, host-b: rolled back -> rolled back",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionRolledBack,
				},
			},
		},
		{
			name:        "host-a: not started -> prepared -> committed, host-b: not started -> prepared -> committed, host-c: not started -> prepared -> committed",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionNotStarted,
				},
				{
					id:    "host-c",
					state: TransactionNotStarted,
				},
			},
		},
		{
			name:        "host-a: not started -> prepared -> committed, host-b: not started -> prepared -> committed, host-c: prepared -> committed",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionNotStarted,
				},
				{
					id:    "host-c",
					state: TransactionPrepared,
				},
			},
		},
		{
			name:        "host-a: not started -> prepared -> committed, host-b: not started -> prepared -> committed, host-c: committed -> committed",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionNotStarted,
				},
				{
					id:    "host-c",
					state: TransactionCommitted,
				},
			},
		},
		{
			name:        "host-a: not started -> not started, host-b: not started -> not started, host-c: prepare failed -> rolled back",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionNotStarted,
				},
				{
					id:    "host-c",
					state: TransactionPrepareFailed,
				},
			},
		},
		{
			name:        "host-a: not started -> not started, host-b: not started -> not started, host-c: rolled back -> rolled back",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionNotStarted,
				},
				{
					id:    "host-c",
					state: TransactionRolledBack,
				},
			},
		},
		{
			name:        "host-a: not started -> prepared -> committed, host-b: prepared -> committed, host-c: prepared -> committed",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepared,
				},
				{
					id:    "host-c",
					state: TransactionPrepared,
				},
			},
		},
		{
			name:        "host-a: not started -> prepared -> committed, host-b: prepared -> committed, host-c: committed -> committed",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepared,
				},
				{
					id:    "host-c",
					state: TransactionCommitted,
				},
			},
		},
		{
			name:        "host-a: not started -> not started, host-b: prepared -> rolled back, host-c: prepare failed -> rolled back",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepared,
				},
				{
					id:    "host-c",
					state: TransactionPrepareFailed,
				},
			},
		},
		{
			name:        "host-a: not started -> not started, host-b: prepared -> rolled back, host-c: rolled back -> rolled back",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepared,
				},
				{
					id:    "host-c",
					state: TransactionRolledBack,
				},
			},
		},
		{
			name:        "host-a: not started -> not started, host-b: committed -> committed, host-c: committed -> committed",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionCommitted,
				},
				{
					id:    "host-c",
					state: TransactionCommitted,
				},
			},
		},
		{
			name:        "host-a: not started -> not started, host-b: prepare failed -> rolled back, host-c: prepare failed -> rolled back",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepareFailed,
				},
				{
					id:    "host-c",
					state: TransactionPrepareFailed,
				},
			},
		},
		{
			name:        "host-a: not started -> not started, host-b: prepare failed -> rolled back, host-c: rolled back -> rolled back",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepareFailed,
				},
				{
					id:    "host-c",
					state: TransactionRolledBack,
				},
			},
		},
		{
			name:        "host-a: not started -> not started, host-b: rolled back -> rolled back, host-c: rolled back -> rolled back",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionRolledBack,
				},
				{
					id:    "host-c",
					state: TransactionRolledBack,
				},
			},
		},
		{
			name:        "host-a: not started -> not started, host-b: prepared -> rolled back, host-c: prepare failed -> rolled back, host-d: rolled back -> rolled back",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepared,
				},
				{
					id:    "host-c",
					state: TransactionPrepareFailed,
				},
				{
					id:    "host-d",
					state: TransactionRolledBack,
				},
			},
		},
		// ═════════════════════════════════════════════════════════════════════════════
		// GROUP 2 - coordinator failures
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name:        "host-a: not started -> prepared -x-> committed",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failCommit(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepare failed -x-> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepareAndRollback(),
				},
			},
		},
		{
			name:        "host-a: prepared -x-> committed",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepared,
					client: failCommit(),
				},
			},
		},
		{
			name:        "host-a: prepare failed -x-> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepareFailed,
					client: failRollback(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepare failed -x-> rolled back, host-b: not started -> prepare failed -x-> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepareAndRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionNotStarted,
					client: failPrepareAndRollback(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepare failed -x-> rolled back, host-b: not started -> prepared -x-> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepareAndRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionNotStarted,
					client: failRollback(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepare failed -x-> rolled back, host-b: not started -> prepare failed -> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepareAndRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionNotStarted,
					client: failPrepare(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepare failed -x-> rolled back, host-b: not started -> prepared -> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepareAndRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionNotStarted,
					client: ok(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepared -x-> rolled back, host-b: not started -> prepare failed -> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionNotStarted,
					client: failPrepare(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepared -x-> committed, host-b: not started -> prepared -x-> committed",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failCommit(),
				},
				{
					id:     "host-b",
					state:  TransactionNotStarted,
					client: failCommit(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepared -x-> committed, host-b: not started -> prepared -> committed",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failCommit(),
				},
				{
					id:     "host-b",
					state:  TransactionNotStarted,
					client: ok(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepare failed -x-> rolled back, host-b: prepared -x-> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepareAndRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepared,
					client: failRollback(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepare failed -x-> rolled back, host-b: prepared -> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepareAndRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepared,
					client: ok(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepare failed -> rolled back, host-b: prepared -x-> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepare(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepared,
					client: failRollback(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepared -x-> committed, host-b: prepared -x-> committed",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failCommit(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepared,
					client: failCommit(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepared -x-> committed, host-b: committed -> committed",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failCommit(),
				},
				{
					id:     "host-b",
					state:  TransactionCommitted,
					client: ok(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepare failed -x-> rolled back, host-b: prepare failed -x-> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepareAndRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepareFailed,
					client: failRollback(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepare failed -x-> rolled back, host-b: prepare failed -> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepareAndRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepareFailed,
					client: ok(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepared -x-> rolled back, host-b: prepare failed -x-> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepareFailed,
					client: failRollback(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepared -x-> rolled back, host-b: prepare failed -> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepareFailed,
					client: ok(),
				},
			},
		},
		{
			name:        "host-a: not started -> prepared -x-> rolled back, host-b: rolled back -> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionRolledBack,
					client: ok(),
				},
			},
		},
		{
			name:        "host-a: prepared -x-> committed, host-b: prepared -x-> committed",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepared,
					client: failCommit(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepared,
					client: failCommit(),
				},
			},
		},
		{
			name:        "host-a: prepared -x-> committed, host-b: prepared -> committed",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepared,
					client: failCommit(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepared,
					client: ok(),
				},
			},
		},
		{
			name:        "host-a: prepared -x-> rolled back, host-b: prepare failed -x-> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepared,
					client: failRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepareFailed,
					client: failRollback(),
				},
			},
		},
		{
			name:        "host-a: prepared -x-> rolled back, host-b: prepare failed -> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepared,
					client: failRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepareFailed,
					client: ok(),
				},
			},
		},
		{
			name:        "host-a: prepared -> rolled back, host-b: prepare failed -x-> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepared,
					client: ok(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepareFailed,
					client: failRollback(),
				},
			},
		},
		{
			name:        "host-a: prepared -x-> committed, host-b: committed -> committed",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepared,
					client: failCommit(),
				},
				{
					id:     "host-b",
					state:  TransactionCommitted,
					client: ok(),
				},
			},
		},
		{
			name:        "host-a: prepared -x-> rolled back, host-b: rolled back -> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepared,
					client: failRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionRolledBack,
					client: ok(),
				},
			},
		},
		{
			name:        "host-a: prepare failed -x-> rolled back, host-b: prepare failed -x-> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepareFailed,
					client: failRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepareFailed,
					client: failRollback(),
				},
			},
		},
		{
			name:        "host-a: prepare failed -x-> rolled back, host-b: prepare failed -> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepareFailed,
					client: failRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepareFailed,
					client: ok(),
				},
			},
		},
		{
			name:        "host-a: prepare failed -x-> rolled back, host-b: rolled back -> rolled back",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepareFailed,
					client: failRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionRolledBack,
					client: ok(),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run(t, newCase(tt))
		})
	}
}

func TestCoordinator_Execute_dependencies_fails(t *testing.T) {
	tests := []testCase{
		{
			name: "committed despite not working persistence and used mixed client construction",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: stubStatePersister[string]{err: errPersist},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": ok(),
					},
					NewClientFunc: newStubClientFunc(
						map[string]Client{
							"host-b": ok(),
						}, nil,
					),
				},
			},
			args: args{
				ctxFunc: ctxWithTimeout(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errPersist},
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "host client not registered, new client fails init, outcome inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newStubClientFunc(nil, errNewClient),
				},
			},
			args: args{
				ctxFunc: ctxWithTimeout(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    []error{errNewClient},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "init state loading fails, outcome inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   stubTransactionStateChecker{err: errStateLoad},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": ok(),
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    []error{errStateLoad},
			wantOutcome: OutcomeInconsistent,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run(t, tt)
		})
	}
}

func run(t *testing.T, tt testCase) {
	coordinator := NewCoordinator(
		tt.fields.persistenceConfig,
		tt.fields.clientConfig,
	)
	result := coordinator.Execute(tt.args.ctxFunc(), tt.args.distributedTransaction)

	fmt.Printf("err: %s", result.Err())
	for _, err := range tt.wantErrs {
		if !errors.Is(result.Err(), err) {
			t.Errorf("Execute() error = %v, wantErr %v", result.Err(), tt.wantErrs)
		}
	}

	if result.Outcome() != tt.wantOutcome {
		t.Errorf("Execute() outcome = %v, wantOutcome %v", result.Outcome(), tt.wantOutcome)
	}
}

type participantConfig struct {
	id     string
	state  TransactionState
	client *stubClient
}

type testCase struct {
	name        string
	fields      fields
	args        args
	wantErrs    []error
	wantOutcome Outcome
}

type fields struct {
	persistenceConfig PersistenceConfig[string]
	clientConfig      ClientConfig[string]
}
type args struct {
	ctxFunc                func() context.Context
	distributedTransaction DistributedTransaction[string]
}

type compactedTestCase struct {
	name         string
	ctxFunc      func() context.Context
	wantOutcome  Outcome
	wantErrs     []error
	participants []participantConfig
}

func newCase(data compactedTestCase) testCase {
	checker, clients, wantErrs, txs := scenario(data.participants...)

	var tc testCase
	tc.name = data.name
	tc.fields.persistenceConfig = PersistenceConfig[string]{
		TransactionStateChecker:   checker,
		TransactionStatePersister: stubStatePersister[string]{},
	}
	tc.fields.clientConfig = ClientConfig[string]{Clients: clients}
	if data.ctxFunc == nil {
		data.ctxFunc = ctxBackground()
	}
	if data.wantOutcome == OutcomeInconsistent {
		data.ctxFunc = ctxWithTimeout()
	}
	tc.args.ctxFunc = data.ctxFunc
	tc.args.distributedTransaction = DistributedTransaction[string]{
		TransactionID: "tx",
		Transactions:  txs,
	}
	tc.wantErrs = append(data.wantErrs, wantErrs...)
	tc.wantOutcome = data.wantOutcome
	return tc
}

func scenario(participants ...participantConfig) (stubTransactionStateChecker, map[string]Client, []error, []Transaction[string]) {
	states := map[string]TransactionState{}
	clients := map[string]Client{}
	var expectedClientsErrs []error
	txs := make([]Transaction[string], 0, len(participants))

	for i, p := range participants {
		if p.state != TransactionNotStarted {
			states[p.id] = p.state
		}
		if p.client == nil {
			p.client = ok()
		}

		if p.client.prepareErr != nil {
			expectedClientsErrs = append(expectedClientsErrs, errPrepare)
		}
		if p.client.commitErr != nil {
			expectedClientsErrs = append(expectedClientsErrs, errCommit)
		} else if p.client.rollbackErr != nil {
			expectedClientsErrs = append(expectedClientsErrs, errRollback)
		}

		clients[p.id] = p.client
		txs = append(txs, Transaction[string]{
			ParticipantID: p.id,
			Payload:       fmt.Sprintf("p%d", i+1),
		})
	}
	return stubTransactionStateChecker{stateByParticipantID: states}, clients, expectedClientsErrs, txs
}

var (
	errPrepare   = errors.New("prepare failed")
	errCommit    = errors.New("commit failed")
	errRollback  = errors.New("rollback failed")
	errPersist   = errors.New("persist failed")
	errNewClient = errors.New("create new client failed")
	errStateLoad = errors.New("state load failed")
)

func ok() *stubClient {
	return &stubClient{}
}

func failPrepare() *stubClient {
	return &stubClient{prepareErr: errPrepare}
}

func failPrepareAndRollback() *stubClient {
	return &stubClient{prepareErr: errPrepare, rollbackErr: errRollback}
}

func failCommit() *stubClient {
	return &stubClient{commitErr: errCommit}
}

func failRollback() *stubClient {
	return &stubClient{rollbackErr: errRollback}
}

func ctxBackground() func() context.Context {
	return func() context.Context {
		return context.Background()
	}
}

func ctxWithTimeout() func() context.Context {
	maxTestTime := 10 * time.Millisecond
	return func() context.Context {
		newCtx, cancel := context.WithTimeout(context.Background(), maxTestTime)
		_ = cancel
		return newCtx
	}
}

type stubStatePersister[ID comparable] struct {
	err error
}

func (sp stubStatePersister[ID]) PersistState(context.Context, string, ID, TransactionState) error {
	return sp.err
}

type stubClient struct {
	prepareErr  error
	commitErr   error
	rollbackErr error
}

func (c *stubClient) PrepareTransaction(_ context.Context, _ string, _ PreparePayload) error {
	return c.prepareErr
}

func (c *stubClient) CommitTransaction(_ context.Context, _ string) error {
	return c.commitErr
}

func (c *stubClient) RollbackTransaction(_ context.Context, _ string) error {
	return c.rollbackErr
}

func newStubClientFunc(hostToClient map[string]Client, err error) func(participantID string) (Client, error) {
	return func(participantID string) (Client, error) {
		if c, ok := hostToClient[participantID]; ok {
			return c, nil
		}
		return nil, err
	}
}

type stubTransactionStateChecker struct {
	stateByParticipantID map[string]TransactionState
	err                  error
}

func (tsc stubTransactionStateChecker) Check(_ context.Context, _ string) (map[string]TransactionState, error) {
	return tsc.stateByParticipantID, tsc.err
}

func allNotStartedChecker() stubTransactionStateChecker {
	return stubTransactionStateChecker{
		stateByParticipantID: map[string]TransactionState{},
	}
}
