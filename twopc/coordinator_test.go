package twopc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestCoordinator_Execute(t *testing.T) {
	type fields struct {
		persistenceConfig PersistenceConfig[string]
		clientConfig      ClientConfig[string]
	}
	type args struct {
		ctxFunc                func() context.Context
		distributedTransaction DistributedTransaction[string]
	}

	errPrepare := errors.New("prepare failed")
	errCommit := errors.New("commit failed")
	errRollback := errors.New("rollback failed")
	errPersist := errors.New("persist failed")
	errPersistCommit := errors.New("persist commit failed")
	errPersistRollback := errors.New("persist rollback failed")
	errNewClient := errors.New("create new client failed")
	errStateLoad := errors.New("state load failed")

	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErrs    []error
		wantOutcome Outcome
	}{
		// ── happy path ────────────────────────────────────────────────────────
		{
			name: "single host: prepare then commit both succeed → no error",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: mockStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newMockNewClientFunc(map[string]Client{
						"host-a": &mockClient{},
					}),
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-1",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "two hosts: prepare then commit both succeed → no error",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: mockStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newMockNewClientFunc(map[string]Client{
						"host-a": &mockClient{},
						"host-b": &mockClient{},
					}),
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-2",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "two hosts, host-a needs to be initialized: prepare then commit both succeed → no error",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: mockStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newMockNewClientFunc(map[string]Client{
						"host-b": &mockClient{},
					}),
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-2",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "two hosts, host-a needs to be initialized: one prepare fails → prepare err",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: mockStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newMockNewClientFunc(map[string]Client{
						"host-b": &mockClient{prepareErr: errPrepare},
					}),
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-2",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errPrepare},
			wantOutcome: OutcomeRolledBack,
		},
		{
			name: "already fully committed initial state → no operations, no error",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: mockTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionCommitted,
						},
					},
					TransactionStatePersister: mockStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newMockNewClientFunc(map[string]Client{
						"host-a": &mockClient{},
					}),
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-3",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "already fully rolled back initial state → returns OutcomeRolledBack",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: mockTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionRolledBack,
						},
					},
					TransactionStatePersister: mockStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newMockNewClientFunc(map[string]Client{
						"host-a": &mockClient{},
					}),
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-4",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeRolledBack,
		},
		{
			name: "resume from prepared: skips prepare, goes straight to commit → no error",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: mockTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepared,
							"host-b": TransactionPrepared,
						},
					},
					TransactionStatePersister: mockStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newMockNewClientFunc(map[string]Client{
						"host-a": &mockClient{},
						"host-b": &mockClient{},
					}),
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-5",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeCommitted,
		},

		// ── error paths ───────────────────────────────────────────────────────
		{
			name: "prepare fails on one host → rollback issued, returns OutcomeRolledBack",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: mockStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newMockNewClientFunc(map[string]Client{
						"host-a": &mockClient{prepareErr: errPrepare},
						"host-b": &mockClient{},
					}),
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-6",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errPrepare},
			wantOutcome: OutcomeRolledBack,
		},
		{
			name: "prepare fails on all hosts → rollback issued, returns OutcomeRolledBack",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: mockStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newMockNewClientFunc(map[string]Client{
						"host-a": &mockClient{prepareErr: errPrepare},
						"host-b": &mockClient{prepareErr: errPrepare},
					}),
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-7",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errPrepare},
			wantOutcome: OutcomeRolledBack,
		},
		{
			name: "persist fails during prepare → returns error, finished 2pc",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: mockStatePersister[string]{err: errPersist},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newMockNewClientFunc(map[string]Client{
						"host-a": &mockClient{},
					}),
				},
			},
			args: args{
				ctxFunc: ctxWithTimeout(context.Background(), 1*time.Second),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-8",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    []error{errPersist},
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "persist rollback fails after client rollback call fails → returns joined errors, didn't finish 2pc",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: mockTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepareFailed,
						},
					},
					TransactionStatePersister: mockStatePersister[string]{rollbackErr: errPersistRollback},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newMockNewClientFunc(map[string]Client{
						"host-a": &mockClient{rollbackErr: errRollback},
					}),
				},
			},
			args: args{
				ctxFunc: ctxWithTimeout(context.Background(), 1*time.Second),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-8",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    []error{errRollback},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "persist fails after client commit call fails → returns joined errors, didn't finish 2pc",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: mockTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepared,
						},
					},
					TransactionStatePersister: mockStatePersister[string]{err: errPersist},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newMockNewClientFunc(map[string]Client{
						"host-a": &mockClient{commitErr: errCommit},
					}),
				},
			},
			args: args{
				ctxFunc: ctxWithTimeout(context.Background(), 1*time.Second),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-8",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    []error{errCommit, errPersist},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "persist commit fails after client commit call → returns persist commit error, finished 2pc",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: mockTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepared,
						},
					},
					TransactionStatePersister: mockStatePersister[string]{commitErr: errPersistCommit},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newMockNewClientFunc(map[string]Client{
						"host-a": &mockClient{},
					}),
				},
			},
			args: args{
				ctxFunc: ctxWithTimeout(context.Background(), 1*time.Second),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-8",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    []error{errPersistCommit},
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "client not registered for host → getClient error → returns error",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: mockStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: func(participantID string) (Client, error) {
						return nil, errNewClient
					},
				},
			},
			args: args{
				ctxFunc: ctxWithTimeout(context.Background(), 1*time.Second),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-9",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    []error{errNewClient},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "failed to load state → abort",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   mockTransactionStateChecker{err: errStateLoad},
					TransactionStatePersister: mockStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newMockNewClientFunc(map[string]Client{
						"host-a": &mockClient{},
					}),
				},
			},
			args: args{
				ctxFunc: ctxWithTimeout(context.Background(), 1*time.Second),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-9",
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
			t.Parallel()
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
		})
	}
}

func ctxBackground() func() context.Context {
	return func() context.Context {
		return context.Background()
	}
}

func ctxWithTimeout(ctx context.Context, timeout time.Duration) func() context.Context {
	return func() context.Context {
		newCtx, cancel := context.WithTimeout(ctx, timeout)
		_ = cancel
		return newCtx
	}
}

type mockStatePersister[ID comparable] struct {
	err         error
	commitErr   error
	rollbackErr error
}

func (m mockStatePersister[ID]) PersistState(context.Context, string, ID, TransactionState) PersistResult {
	if m.err != nil {
		return PersistResult{Err: m.err}
	}
	return PersistResult{
		Commit:   func() error { return m.commitErr },
		Rollback: func() error { return m.rollbackErr },
	}
}

type mockClient struct {
	prepareErr  error
	commitErr   error
	rollbackErr error
}

func (m *mockClient) PrepareTransaction(_ context.Context, _ string, _ PreparePayload) error {
	return m.prepareErr
}

func (m *mockClient) CommitTransaction(_ context.Context, _ string) error {
	return m.commitErr
}

func (m *mockClient) RollbackTransaction(_ context.Context, _ string) error {
	return m.rollbackErr
}

func newMockNewClientFunc(hostToClient map[string]Client) func(participantID string) (Client, error) {
	return func(participantID string) (Client, error) {
		if c, ok := hostToClient[participantID]; ok {
			return c, nil
		}
		return &mockClient{}, nil
	}
}

type mockTransactionStateChecker struct {
	stateByParticipantID map[string]TransactionState
	err                  error
}

func (m mockTransactionStateChecker) Check(_ context.Context, _ string) (map[string]TransactionState, error) {
	return m.stateByParticipantID, m.err
}

func allNotStartedChecker() mockTransactionStateChecker {
	return mockTransactionStateChecker{
		stateByParticipantID: map[string]TransactionState{},
	}
}
