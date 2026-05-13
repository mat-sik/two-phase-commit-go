package twopc

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCoordinator_Execute(t *testing.T) {
	type fields struct {
		transactionStateChecker TransactionStateChecker[string]
		statePersister          StatePersister[string]
		newClientFunc           func(clientID string) (Client, error)
	}
	type args struct {
		ctx                    context.Context
		distributedTransaction DistributedTransaction[string]
	}

	prepareErr := errors.New("prepare failed")
	persistErr := errors.New("persist failed")

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
	}{
		// ── happy path ────────────────────────────────────────────────────────
		{
			name: "single host: prepare then commit both succeed → no error",
			fields: fields{
				transactionStateChecker: allNotStartedChecker(),
				statePersister:          mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-1",
					Transactions: []Transaction[string]{
						{ClientID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "two hosts: prepare then commit both succeed → no error",
			fields: fields{
				transactionStateChecker: allNotStartedChecker(),
				statePersister:          mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{},
					"host-b": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-2",
					Transactions: []Transaction[string]{
						{ClientID: "host-a", Payload: "p1"},
						{ClientID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "two hosts, host-a needs to be initialized: prepare then commit both succeed → no error",
			fields: fields{
				transactionStateChecker: allNotStartedChecker(),
				statePersister:          mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-b": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-2",
					Transactions: []Transaction[string]{
						{ClientID: "host-a", Payload: "p1"},
						{ClientID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "already fully committed initial state → no operations, no error",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByClientID: map[string]TransactionState{
						"host-a": Committed,
					},
				},
				statePersister: mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-3",
					Transactions: []Transaction[string]{
						{ClientID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "already fully rolled back initial state → returns ErrRollback",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByClientID: map[string]TransactionState{
						"host-a": RolledBack,
					},
				},
				statePersister: mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-4",
					Transactions: []Transaction[string]{
						{ClientID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErr: ErrRollback,
		},
		{
			name: "resume from prepared: skips prepare, goes straight to commit → no error",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByClientID: map[string]TransactionState{
						"host-a": Prepared,
						"host-b": Prepared,
					},
				},
				statePersister: mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{},
					"host-b": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-5",
					Transactions: []Transaction[string]{
						{ClientID: "host-a", Payload: "p1"},
						{ClientID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErr: nil,
		},

		// ── error paths ───────────────────────────────────────────────────────
		{
			name: "prepare fails on one host → rollback issued, returns ErrRollback",
			fields: fields{
				transactionStateChecker: allNotStartedChecker(),
				statePersister:          mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{prepareErr: prepareErr},
					"host-b": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-6",
					Transactions: []Transaction[string]{
						{ClientID: "host-a", Payload: "p1"},
						{ClientID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErr: ErrRollback,
		},
		{
			name: "prepare fails on all hosts → rollback issued, returns ErrRollback",
			fields: fields{
				transactionStateChecker: allNotStartedChecker(),
				statePersister:          mockStatePersister[string]{},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{prepareErr: prepareErr},
					"host-b": &mockClient{prepareErr: prepareErr},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-7",
					Transactions: []Transaction[string]{
						{ClientID: "host-a", Payload: "p1"},
						{ClientID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErr: ErrRollback,
		},
		{
			name: "persist fails during prepare → returns error",
			fields: fields{
				transactionStateChecker: allNotStartedChecker(),
				statePersister:          mockStatePersister[string]{err: persistErr},
				newClientFunc: newMockNewClientFunc(map[string]Client{
					"host-a": &mockClient{},
				}),
			},
			args: args{
				ctx: ctxWithTimeout(context.Background(), time.Second),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-8",
					Transactions: []Transaction[string]{
						{ClientID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErr: errAny,
		},
		{
			name: "client not registered for host → getClient error → returns error",
			fields: fields{
				transactionStateChecker: allNotStartedChecker(),
				statePersister:          mockStatePersister[string]{},
				newClientFunc: func(clientID string) (Client, error) {
					return nil, errors.New("failed to create new client")
				},
			},
			args: args{
				ctx: ctxWithTimeout(context.Background(), time.Second),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx-9",
					Transactions: []Transaction[string]{
						{ClientID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErr: errAny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coordinator := NewCoordinator(
				tt.fields.transactionStateChecker,
				tt.fields.statePersister,
				tt.fields.newClientFunc,
			)
			err := coordinator.Execute(tt.args.ctx, tt.args.distributedTransaction)
			if errors.Is(tt.wantErr, errAny) && err != nil {
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

var errAny = errors.New("any error")

func ctxWithTimeout(ctx context.Context, timeout time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	_ = cancel
	return ctx
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

func newMockNewClientFunc(hostToClient map[string]Client) func(clientID string) (Client, error) {
	return func(clientID string) (Client, error) {
		if c, ok := hostToClient[clientID]; ok {
			return c, nil
		}
		return &mockClient{}, nil
	}
}

type mockTransactionStateChecker struct {
	stateByClientID map[string]TransactionState
}

func (m mockTransactionStateChecker) Check(_ string) map[string]TransactionState {
	return m.stateByClientID
}

func allNotStartedChecker() mockTransactionStateChecker {
	return mockTransactionStateChecker{
		stateByClientID: map[string]TransactionState{},
	}
}
