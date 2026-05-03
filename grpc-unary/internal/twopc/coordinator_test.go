package twopc

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOperationHandler_HandleRequest(t *testing.T) {
	type fields struct {
		stateLoader     StateLoader
		statePersister  StatePersister
		clientRegistrar clientRegistrar
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
				clientRegistrar: newMockClientRegistrar(map[ClientID]Client{
					"host-a": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-1",
					Transactions:  []Transaction{{ClientID: "host-a", Payload: "p1"}},
				},
			},
			wantErr: false,
		},
		{
			name: "two hosts: prepare then commit both succeed → no error",
			fields: fields{
				stateLoader:    allNotStartedLoader(),
				statePersister: mockStatePersister{},
				clientRegistrar: newMockClientRegistrar(map[ClientID]Client{
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
				clientRegistrar: newMockClientRegistrar(map[ClientID]Client{
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
				stateLoader: StateLoader{
					transactionStateChecker: mockTransactionStateChecker{
						stateByClientID: map[ClientID]TransactionState{
							"host-a": transactionCommitted,
						},
					},
				},
				statePersister: mockStatePersister{},
				clientRegistrar: newMockClientRegistrar(map[ClientID]Client{
					"host-a": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-3",
					Transactions:  []Transaction{{ClientID: "host-a", Payload: "p1"}},
				},
			},
			wantErr: false,
		},
		{
			name: "already fully rolled back initial state → no operations, no error",
			fields: fields{
				stateLoader: StateLoader{
					transactionStateChecker: mockTransactionStateChecker{
						stateByClientID: map[ClientID]TransactionState{
							"host-a": transactionRolledBack,
						},
					},
				},
				statePersister: mockStatePersister{},
				clientRegistrar: newMockClientRegistrar(map[ClientID]Client{
					"host-a": &mockClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-4",
					Transactions:  []Transaction{{ClientID: "host-a", Payload: "p1"}},
				},
			},
			wantErr: false,
		},
		{
			name: "resume from prepared: skips prepare, goes straight to commit → no error",
			fields: fields{
				stateLoader: StateLoader{
					transactionStateChecker: mockTransactionStateChecker{
						stateByClientID: map[ClientID]TransactionState{
							"host-a": transactionPrepared,
							"host-b": transactionPrepared,
						},
					},
				},
				statePersister: mockStatePersister{},
				clientRegistrar: newMockClientRegistrar(map[ClientID]Client{
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
				clientRegistrar: newMockClientRegistrar(map[ClientID]Client{
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
				clientRegistrar: newMockClientRegistrar(map[ClientID]Client{
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
				clientRegistrar: newMockClientRegistrar(map[ClientID]Client{
					"host-a": &mockClient{},
				}),
			},
			args: args{
				ctx: ctxWithTimeout(context.Background(), time.Second),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-8",
					Transactions:  []Transaction{{ClientID: "host-a", Payload: "p1"}},
				},
			},
			wantErr: true,
		},
		{
			name: "client not registered for host → getClient error → returns error",
			fields: fields{
				stateLoader:     allNotStartedLoader(),
				statePersister:  mockStatePersister{},
				clientRegistrar: newMockFailingOnNewClientClientRegistrar(map[ClientID]Client{}),
			},
			args: args{
				ctx: ctxWithTimeout(context.Background(), time.Second),
				distributedTransaction: DistributedTransaction{
					TransactionID: "tx-9",
					Transactions:  []Transaction{{ClientID: "host-a", Payload: "p1"}},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coordinator := Coordinator{
				stateLoader:     tt.fields.stateLoader,
				statePersister:  tt.fields.statePersister,
				clientRegistrar: tt.fields.clientRegistrar,
			}
			if err := coordinator.Execute(tt.args.ctx, tt.args.distributedTransaction); (err != nil) != tt.wantErr {
				t.Errorf("HandleRequest() error = %v, wantErr %v", err, tt.wantErr)
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

func (m mockStatePersister) PersistState(_ context.Context, _ string, _ ClientID, _ TransactionState) <-chan PersistResult {
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

func (m mockClient) prepareTransaction(_ context.Context, _ string, _ prepareOperation) error {
	return m.prepareErr
}

func (m mockClient) commitTransaction(_ context.Context, _ string) error {
	return m.commitErr
}

func (m mockClient) rollbackTransaction(_ context.Context, _ string) error {
	return m.rollbackErr
}

// newMockFailingOnNewClientClientRegistrar always fails on creating new client
func newMockFailingOnNewClientClientRegistrar(hostToClient map[ClientID]Client) clientRegistrar {
	newClient := func(identifiable ClientRegistrarUsable) (Client, error) {
		return mockClient{}, errors.New("failed to create new client")
	}
	return newMockClientRegistrarBase(hostToClient, newClient)
}

// newMockClientRegistrar never fails on creating new client
func newMockClientRegistrar(hostToClient map[ClientID]Client) clientRegistrar {
	newClient := func(identifiable ClientRegistrarUsable) (Client, error) {
		return mockClient{}, nil
	}
	return newMockClientRegistrarBase(hostToClient, newClient)
}

func newMockClientRegistrarBase(
	hostToClient map[ClientID]Client,
	newClientFunc func(identifiable ClientRegistrarUsable) (Client, error),
) clientRegistrar {
	cr := clientRegistrar{
		store:     &clientRegistrarStore{},
		newClient: newClientFunc,
	}
	for cID, c := range hostToClient {
		cr.store.add(cID, c)
	}
	return cr
}

func allNotStartedLoader() StateLoader {
	return StateLoader{
		transactionStateChecker: mockTransactionStateChecker{
			stateByClientID: map[ClientID]TransactionState{},
		},
	}
}
