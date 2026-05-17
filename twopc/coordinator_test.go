package twopc

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCoordinator_Execute(t *testing.T) {
	type fields struct {
		transactionStateChecker   TransactionStateChecker[string]
		transactionStatePersister TransactionStatePersister[string]
		newClientFunc             func(participantID string) (Client, error)
	}
	type args struct {
		ctxFunc                func() context.Context
		distributedTransaction DistributedTransaction[string]
	}

	errPrepare := errors.New("prepare failed")
	errPersist := errors.New("persist failed")
	errNewClient := errors.New("create new client failed")
	errStateLoad := errors.New("state load failed")

	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     error
		wantOutcome Outcome
	}{
		// ── happy path ────────────────────────────────────────────────────────
		{
			name: "single host: prepare then commit both succeed → no error",
			fields: fields{
				transactionStateChecker:   allNotStartedChecker(),
				transactionStatePersister: mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{},
				}),
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
			wantErr:     nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "two hosts: prepare then commit both succeed → no error",
			fields: fields{
				transactionStateChecker:   allNotStartedChecker(),
				transactionStatePersister: mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{},
					"host-b": &mockClient{},
				}),
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
			wantErr:     nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "two hosts, host-a needs to be initialized: prepare then commit both succeed → no error",
			fields: fields{
				transactionStateChecker:   allNotStartedChecker(),
				transactionStatePersister: mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-b": &mockClient{},
				}),
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
			wantErr:     nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "already fully committed initial state → no operations, no error",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByParticipantID: map[string]TransactionState{
						"host-a": TransactionCommitted,
					},
				},
				transactionStatePersister: mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{},
				}),
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
			wantErr:     nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "already fully rolled back initial state → returns OutcomeRolledBack",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByParticipantID: map[string]TransactionState{
						"host-a": TransactionRolledBack,
					},
				},
				transactionStatePersister: mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{},
				}),
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
			wantErr:     nil,
			wantOutcome: OutcomeRolledBack,
		},
		{
			name: "resume from prepared: skips prepare, goes straight to commit → no error",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByParticipantID: map[string]TransactionState{
						"host-a": TransactionPrepared,
						"host-b": TransactionPrepared,
					},
				},
				transactionStatePersister: mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{},
					"host-b": &mockClient{},
				}),
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
			wantErr:     nil,
			wantOutcome: OutcomeCommitted,
		},

		// ── error paths ───────────────────────────────────────────────────────
		{
			name: "prepare fails on one host → rollback issued, returns OutcomeRolledBack",
			fields: fields{
				transactionStateChecker:   allNotStartedChecker(),
				transactionStatePersister: mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{prepareErr: errPrepare},
					"host-b": &mockClient{},
				}),
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
			wantErr:     errPrepare,
			wantOutcome: OutcomeRolledBack,
		},
		{
			name: "prepare fails on all hosts → rollback issued, returns OutcomeRolledBack",
			fields: fields{
				transactionStateChecker:   allNotStartedChecker(),
				transactionStatePersister: mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{prepareErr: errPrepare},
					"host-b": &mockClient{prepareErr: errPrepare},
				}),
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
			wantErr:     errPrepare,
			wantOutcome: OutcomeRolledBack,
		},
		{
			name: "persist fails during prepare → returns error",
			fields: fields{
				transactionStateChecker:   allNotStartedChecker(),
				transactionStatePersister: mockStatePersister[string]{err: errPersist},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{},
				}),
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
			wantErr:     errPersist,
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "client not registered for host → getClient error → returns error",
			fields: fields{
				transactionStateChecker:   allNotStartedChecker(),
				transactionStatePersister: mockStatePersister[string]{},
				newClientFunc: func(participantID string) (Client, error) {
					return nil, errNewClient
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
			wantErr:     errNewClient,
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "failed to load state → abort",
			fields: fields{
				transactionStateChecker:   mockTransactionStateChecker{err: errStateLoad},
				transactionStatePersister: mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{},
				}),
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
			wantErr:     errStateLoad,
			wantOutcome: OutcomeInconsistent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coordinator := NewCoordinator(
				tt.fields.transactionStateChecker,
				tt.fields.transactionStatePersister,
				tt.fields.newClientFunc,
			)
			result := coordinator.Execute(tt.args.ctxFunc(), tt.args.distributedTransaction)

			if !errors.Is(result.Err(), tt.wantErr) {
				t.Errorf("Execute() error = %v, wantErr %v", result.Err(), tt.wantErr)
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
	err error
}

func (m mockStatePersister[ID]) PersistState(_ context.Context, _ string, _ ID, _ TransactionState) <-chan PersistResult {
	ch := make(chan PersistResult, 1)
	if m.err != nil {
		ch <- PersistResult{Err: m.err}
	} else {
		ch <- PersistResult{
			Commit:   func() error { return nil },
			Rollback: func() error { return nil },
		}
	}
	return ch
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

func (m mockTransactionStateChecker) Check(_ string) (map[string]TransactionState, error) {
	return m.stateByParticipantID, m.err
}

func allNotStartedChecker() mockTransactionStateChecker {
	return mockTransactionStateChecker{
		stateByParticipantID: map[string]TransactionState{},
	}
}
