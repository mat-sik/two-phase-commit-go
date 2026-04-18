package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/mat-sik/two-phase-commit-go/grpc-unary/internal/generated/client/v1"
	"google.golang.org/grpc"
)

func TestOperationHandler_HandleRequest(t *testing.T) {
	type fields struct {
		stateLoader     stateLoader
		statePersister  statePersister
		clientRegistrar clientRegistrar
	}
	type args struct {
		ctx     context.Context
		request AtomicTransactions
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
				clientRegistrar: newClientRegistrar(map[string]pb.ClientServiceClient{
					"host-a": &mockGRPCClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				request: AtomicTransactions{
					transactionID: "tx-1",
					transactions:  []Transaction{{targetHost: "host-a", payload: "p1"}},
				},
			},
			wantErr: false,
		},
		{
			name: "two hosts: prepare then commit both succeed → no error",
			fields: fields{
				stateLoader:    allNotStartedLoader(),
				statePersister: mockStatePersister{},
				clientRegistrar: newClientRegistrar(map[string]pb.ClientServiceClient{
					"host-a": &mockGRPCClient{},
					"host-b": &mockGRPCClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				request: AtomicTransactions{
					transactionID: "tx-2",
					transactions: []Transaction{
						{targetHost: "host-a", payload: "p1"},
						{targetHost: "host-b", payload: "p2"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "already fully committed initial state → no operations, no error",
			fields: fields{
				stateLoader: stateLoader{
					transactionStateChecker: mockTransactionStateChecker{
						stateByHost: map[string]transactionState{
							"host-a": transactionCommitted,
						},
					},
				},
				statePersister: mockStatePersister{},
				clientRegistrar: newClientRegistrar(map[string]pb.ClientServiceClient{
					"host-a": &mockGRPCClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				request: AtomicTransactions{
					transactionID: "tx-3",
					transactions:  []Transaction{{targetHost: "host-a", payload: "p1"}},
				},
			},
			wantErr: false,
		},
		{
			name: "already fully rolled back initial state → no operations, no error",
			fields: fields{
				stateLoader: stateLoader{
					transactionStateChecker: mockTransactionStateChecker{
						stateByHost: map[string]transactionState{
							"host-a": transactionRolledBack,
						},
					},
				},
				statePersister: mockStatePersister{},
				clientRegistrar: newClientRegistrar(map[string]pb.ClientServiceClient{
					"host-a": &mockGRPCClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				request: AtomicTransactions{
					transactionID: "tx-4",
					transactions:  []Transaction{{targetHost: "host-a", payload: "p1"}},
				},
			},
			wantErr: false,
		},
		{
			name: "resume from prepared: skips prepare, goes straight to commit → no error",
			fields: fields{
				stateLoader: stateLoader{
					transactionStateChecker: mockTransactionStateChecker{
						stateByHost: map[string]transactionState{
							"host-a": transactionPrepared,
							"host-b": transactionPrepared,
						},
					},
				},
				statePersister: mockStatePersister{},
				clientRegistrar: newClientRegistrar(map[string]pb.ClientServiceClient{
					"host-a": &mockGRPCClient{},
					"host-b": &mockGRPCClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				request: AtomicTransactions{
					transactionID: "tx-5",
					transactions: []Transaction{
						{targetHost: "host-a", payload: "p1"},
						{targetHost: "host-b", payload: "p2"},
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
				clientRegistrar: newClientRegistrar(map[string]pb.ClientServiceClient{
					"host-a": &mockGRPCClient{prepareErr: prepareErr},
					"host-b": &mockGRPCClient{},
				}),
			},
			args: args{
				ctx: context.Background(),
				request: AtomicTransactions{
					transactionID: "tx-6",
					transactions: []Transaction{
						{targetHost: "host-a", payload: "p1"},
						{targetHost: "host-b", payload: "p2"},
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
				clientRegistrar: newClientRegistrar(map[string]pb.ClientServiceClient{
					"host-a": &mockGRPCClient{prepareErr: prepareErr},
					"host-b": &mockGRPCClient{prepareErr: prepareErr},
				}),
			},
			args: args{
				ctx: context.Background(),
				request: AtomicTransactions{
					transactionID: "tx-7",
					transactions: []Transaction{
						{targetHost: "host-a", payload: "p1"},
						{targetHost: "host-b", payload: "p2"},
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
				clientRegistrar: newClientRegistrar(map[string]pb.ClientServiceClient{
					"host-a": &mockGRPCClient{},
				}),
			},
			args: args{
				ctx: ctxWithTimeout(context.Background(), time.Second),
				request: AtomicTransactions{
					transactionID: "tx-8",
					transactions:  []Transaction{{targetHost: "host-a", payload: "p1"}},
				},
			},
			wantErr: true,
		},
		{
			name: "client not registered for host → getClient error → returns error",
			fields: fields{
				stateLoader:     allNotStartedLoader(),
				statePersister:  mockStatePersister{},
				clientRegistrar: newClientRegistrar(map[string]pb.ClientServiceClient{}), // empty — no hosts registered
			},
			args: args{
				ctx: ctxWithTimeout(context.Background(), time.Second),
				request: AtomicTransactions{
					transactionID: "tx-9",
					transactions:  []Transaction{{targetHost: "host-a", payload: "p1"}},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oh := OperationHandler{
				stateLoader:     tt.fields.stateLoader,
				statePersister:  tt.fields.statePersister,
				clientRegistrar: tt.fields.clientRegistrar,
			}
			if err := oh.HandleRequest(tt.args.ctx, tt.args.request); (err != nil) != tt.wantErr {
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
	// err is returned as persistResult.err for every call when non-nil.
	// When nil, a successful commit (no-op) is returned.
	err error
}

func (m mockStatePersister) persistState(_ context.Context, _ string, _ string, _ transactionState) <-chan persistResult {
	ch := make(chan persistResult, 1)
	if m.err != nil {
		ch <- persistResult{err: m.err}
	} else {
		ch <- persistResult{
			commit:   func() error { return nil },
			rollback: func() error { return nil },
		}
	}
	return ch
}

// mockGRPCClient implements pb.ClientServiceClient.
// Each operation returns the configured error (nil = success).
type mockGRPCClient struct {
	prepareErr  error
	commitErr   error
	rollbackErr error
}

func (m *mockGRPCClient) PrepareTransaction(_ context.Context, _ *pb.PrepareTransactionRequest, _ ...grpc.CallOption) (*pb.PrepareTransactionResponse, error) {
	return &pb.PrepareTransactionResponse{}, m.prepareErr
}

func (m *mockGRPCClient) CommitTransaction(_ context.Context, _ *pb.CommitTransactionRequest, _ ...grpc.CallOption) (*pb.CommitTransactionResponse, error) {
	return &pb.CommitTransactionResponse{}, m.commitErr
}

func (m *mockGRPCClient) RollbackTransaction(_ context.Context, _ *pb.RollbackTransactionRequest, _ ...grpc.CallOption) (*pb.RollbackTransactionResponse, error) {
	return &pb.RollbackTransactionResponse{}, m.rollbackErr
}

// newClientRegistrar pre-populates the store with the provided mock so that
// getClient never dials a real connection.
func newClientRegistrar(hostToClient map[string]pb.ClientServiceClient) clientRegistrar {
	cr := clientRegistrar{store: &clientRegistrarStore{}}
	for host, client := range hostToClient {
		cr.store.add(host, client)
	}
	return cr
}

// allNotStartedLoader returns a stateLoader whose checker reports every host
// as not started, so HandleRequest always begins from a clean slate.
func allNotStartedLoader() stateLoader {
	return stateLoader{
		transactionStateChecker: mockTransactionStateChecker{
			stateByHost: map[string]transactionState{},
		},
	}
}
