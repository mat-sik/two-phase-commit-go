//go:build testcontainers

package test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/basic"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/adapter"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func Test_postgres_persistence_basic_logic_gRPC(t *testing.T) {
	t.Parallel()
	tests := []testContainersTestCase[*adapter.GRPCBasicHandler]{
		{
			name: "happy path",
			handlersConfig: handlersConfig[*adapter.GRPCBasicHandler]{
				handlers: []*adapter.GRPCBasicHandler{
					adapter.NewBasicGRPCHandler(),
					adapter.NewBasicGRPCHandler(),
					adapter.NewBasicGRPCHandler(),
				},
				mapper: basicGRPCServerRequests,
			},
			coordinatorConfig: testContainersCoordinatorConfig{
				persistenceConfigProvider: coordinator.NewPostgresPersistenceConfig,
				clientConfig: coordinatorClientConfig{
					clientConfigProvider: newClientConfig(coordinator.NewBasicGRPCClient()),
				},
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-postgres-basic-grpc-1",
				transactions: []transaction{
					{
						participantNumber: 0,
						payload:           "one",
					},
					{
						participantNumber: 1,
						payload:           "two",
					},
					{
						participantNumber: 2,
						payload:           "three",
					},
				},
			},
			wantErr:       false,
			wantedOutcome: twopc.OutcomeCommitted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runTestContainersTest(t, tt)
		})
	}
}

func Test_postgres_persistence_basic_logic_REST(t *testing.T) {
	t.Parallel()
	tests := []testContainersTestCase[*http.ServeMux]{
		{
			name: "happy path",
			handlersConfig: handlersConfig[*http.ServeMux]{
				handlers: []*http.ServeMux{
					adapter.NewBasicMux(),
					adapter.NewBasicMux(),
					adapter.NewBasicMux(),
				},
				mapper: restServerRequests,
			},
			coordinatorConfig: testContainersCoordinatorConfig{
				persistenceConfigProvider: coordinator.NewPostgresPersistenceConfig,
				clientConfig: coordinatorClientConfig{
					clientConfigProvider: newClientConfig(coordinator.NewRestClient()),
				},
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-postgres-basic-REST-1",
				transactions: []transaction{
					{
						participantNumber: 0,
						payload: basic.PreparePayload{
							Payload:   "one",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 1,
						payload: basic.PreparePayload{
							Payload:   "two",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 2,
						payload: basic.PreparePayload{
							Payload:   "three",
							CreatedAt: time.Now(),
						},
					},
				},
			},
			wantErr:       false,
			wantedOutcome: twopc.OutcomeCommitted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runTestContainersTest(t, tt)
		})
	}
}

func Test_postgres_persistence_basic_logic_gRPC_eventual_consistency(t *testing.T) {
	t.Parallel()

	coordinatorPool, coordinatorPostgresTerminator, err := runPostgresForCoordinatorPool(t.Context())
	if err != nil {
		t.Fatalf("failed to run coordinator postgres container: %v", err)
	}
	t.Cleanup(coordinatorPostgresTerminator)

	txCoordinator := coordinator.NewPostgresBasicGRPCCoordinator(
		coordinatorPool,
		twopc.WithBackoffMax(200*time.Millisecond),
	)
	tx := distributedTransaction{
		transactionID: "tx-postgres-basic-grpc-eventual-consistency-1",
		transactions: []transaction{
			{participantNumber: 0, payload: "one"},
			{participantNumber: 1, payload: "two"},
			{participantNumber: 2, payload: "three"},
		},
	}
	srvConfig := basicGRPCServerRequests([]*adapter.GRPCBasicHandler{
		adapter.NewFailingBasicGRPCHandler(0, 15, 0),
		adapter.NewFailingBasicGRPCHandler(0, 20, 0),
		adapter.NewFailingBasicGRPCHandler(0, 30, 0),
	})
	srvBundle, err := runServers(srvConfig)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	t.Cleanup(func() {
		errs := srvBundle.shutdown()
		if len(errs) != 0 {
			t.Errorf("got %d server errors: %v", len(errs), errs)
		}
	})

	testCtx, testCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer testCancel()
	for {
		if err = testCtx.Err(); err != nil {
			t.Fatalf("failed to eventually finish in committed state, err: %v", err)
		}
		ctx, cancel := context.WithTimeout(testCtx, 1*time.Second)
		addresses := srvBundle.addresses()
		outcome := txCoordinator.Execute(ctx, tx.toTwopc(addresses))
		cancel()
		if outcome.Outcome() == twopc.OutcomeCommitted {
			break
		}
	}
}
