package twopc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/client"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/state"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

func TestCoordinator_Execute(t *testing.T) {
	type fields struct {
		stateLoader    state.Loader
		statePersister StatePersister
		newClientFunc  func(clientID client.ID) (client.Client, error)
	}
	type args struct {
		ctx                    context.Context
		distributedTransaction DistributedTransaction
	}

	prepareErr := errors.New("prepare failed")
	persistErr := errors.New("persist failed")

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// ── happy path ────────────────────────────────────────────────────────
		{
			name: "single host: prepare then commit both succeed → no error",
			fields: fields{
				stateLoader:    allNotStartedLoader(),
				statePersister: mockStatePersister{},
				newClientFunc: newMockNewClientFunc(map[client.ID]client.Client{
					"host-a": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-1",
					Transactions: []Transaction{
						{ClientID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "two hosts: prepare then commit both succeed → no error",
			fields: fields{
				stateLoader:    allNotStartedLoader(),
				statePersister: mockStatePersister{},
				newClientFunc: newMockNewClientFunc(map[client.ID]client.Client{
					"host-a": &mockClient{},
					"host-b": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-2",
					Transactions: []Transaction{
						{ClientID: "host-a", Payload: "p1"},
						{ClientID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "two hosts, host-a needs to be initialized: prepare then commit both succeed → no error",
			fields: fields{
				stateLoader:    allNotStartedLoader(),
				statePersister: mockStatePersister{},
				newClientFunc: newMockNewClientFuncWithFallback(map[client.ID]client.Client{
					"host-b": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-2",
					Transactions: []Transaction{
						{ClientID: "host-a", Payload: "p1"},
						{ClientID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "already fully committed initial state → no operations, no error",
			fields: fields{
				stateLoader: state.NewLoader(mockTransactionStateChecker{
					stateByClientID: map[client.ID]transaction.State{
						"host-a": transaction.Committed,
					},
				}),
				statePersister: mockStatePersister{},
				newClientFunc: newMockNewClientFunc(map[client.ID]client.Client{
					"host-a": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-3",
					Transactions: []Transaction{
						{ClientID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "already fully rolled back initial state → no operations, no error",
			fields: fields{
				stateLoader: state.NewLoader(mockTransactionStateChecker{
					stateByClientID: map[client.ID]transaction.State{
						"host-a": transaction.RolledBack,
					},
				}),
				statePersister: mockStatePersister{},
				newClientFunc: newMockNewClientFunc(map[client.ID]client.Client{
					"host-a": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-4",
					Transactions: []Transaction{
						{ClientID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "resume from prepared: skips prepare, goes straight to commit → no error",
			fields: fields{
				stateLoader: state.NewLoader(mockTransactionStateChecker{
					stateByClientID: map[client.ID]transaction.State{
						"host-a": transaction.Prepared,
						"host-b": transaction.Prepared,
					},
				}),
				statePersister: mockStatePersister{},
				newClientFunc: newMockNewClientFunc(map[client.ID]client.Client{
					"host-a": &mockClient{},
					"host-b": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-5",
					Transactions: []Transaction{
						{ClientID: "host-a", Payload: "p1"},
						{ClientID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErr: false,
		},

		// ── error paths ───────────────────────────────────────────────────────
		{
			name: "prepare fails on one host → rollback issued, returns error",
			fields: fields{
				stateLoader:    allNotStartedLoader(),
				statePersister: mockStatePersister{},
				newClientFunc: newMockNewClientFunc(map[client.ID]client.Client{
					"host-a": &mockClient{prepareErr: prepareErr},
					"host-b": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-6",
					Transactions: []Transaction{
						{ClientID: "host-a", Payload: "p1"},
						{ClientID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "prepare fails on all hosts → rollback issued, returns error",
			fields: fields{
				stateLoader:    allNotStartedLoader(),
				statePersister: mockStatePersister{},
				newClientFunc: newMockNewClientFunc(map[client.ID]client.Client{
					"host-a": &mockClient{prepareErr: prepareErr},
					"host-b": &mockClient{prepareErr: prepareErr},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-7",
					Transactions: []Transaction{
						{ClientID: "host-a", Payload: "p1"},
						{ClientID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "persist fails during prepare → returns error",
			fields: fields{
				stateLoader:    allNotStartedLoader(),
				statePersister: mockStatePersister{err: persistErr},
				newClientFunc: newMockNewClientFunc(map[client.ID]client.Client{
					"host-a": &mockClient{},
				}),
			},
			args: args{
				ctx: ctxWithTimeout(context.Background(), time.Second),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-8",
					Transactions: []Transaction{
						{ClientID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "client not registered for host → getClient error → returns error",
			fields: fields{
				stateLoader:    allNotStartedLoader(),
				statePersister: mockStatePersister{},
				newClientFunc: func(clientID client.ID) (client.Client, error) {
					return nil, errors.New("failed to create new client")
				},
			},
			args: args{
				ctx: ctxWithTimeout(context.Background(), time.Second),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-9",
					Transactions: []Transaction{
						{ClientID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coordinator := NewCoordinator(
				tt.fields.stateLoader,
				tt.fields.statePersister,
				tt.fields.newClientFunc,
			)
			if err := coordinator.Execute(tt.args.ctx, tt.args.distributedTransaction); (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func ctxWithTimeout(ctx context.Context, timeout time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	_ = cancel
	return ctx
}

type mockStatePersister struct {
	err error
}

func (m mockStatePersister) PersistState(_ context.Context, _ string, _ client.ID, _ transaction.State) <-chan PersistResult {
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

func (m mockClient) PrepareTransaction(_ context.Context, _ string, _ client.PreparePayload) error {
	return m.prepareErr
}

func (m mockClient) CommitTransaction(_ context.Context, _ string) error {
	return m.commitErr
}

func (m mockClient) RollbackTransaction(_ context.Context, _ string) error {
	return m.rollbackErr
}

func (m mockClient) ClientIdentifier() client.ID {
	return "mock-client"
}

func newMockNewClientFunc(hostToClient map[client.ID]client.Client) func(clientID client.ID) (client.Client, error) {
	return func(clientID client.ID) (client.Client, error) {
		if c, ok := hostToClient[clientID]; ok {
			return c, nil
		}
		return &mockClient{}, nil
	}
}

func newMockNewClientFuncWithFallback(hostToClient map[client.ID]client.Client) func(clientID client.ID) (client.Client, error) {
	return func(clientID client.ID) (client.Client, error) {
		if c, ok := hostToClient[clientID]; ok {
			return c, nil
		}
		return &mockClient{}, nil
	}
}

type mockTransactionStateChecker struct {
	stateByClientID map[client.ID]transaction.State
}

func (m mockTransactionStateChecker) Check(_ string) map[client.ID]transaction.State {
	return m.stateByClientID
}

func allNotStartedLoader() state.Loader {
	return state.NewLoader(mockTransactionStateChecker{
		stateByClientID: map[client.ID]transaction.State{},
	})
}
