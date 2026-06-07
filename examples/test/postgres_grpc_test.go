//go:build testcontainers

package test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/adapter"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func Test_grpc_sql_basic_integration(t *testing.T) {
	t.Parallel()
	tests := []testContainersTestCase[*adapter.GRPCBasicHandler]{
		{
			name: "simple postgres noop gRPC happy path",
			handlers: []*adapter.GRPCBasicHandler{
				adapter.NewBasicGRPCHandler(),
				adapter.NewBasicGRPCHandler(),
				adapter.NewBasicGRPCHandler(),
			},
			handlersMapper: basicGRPCServerRequests,
			txCoordinatorProvider: func(pool *pgxpool.Pool) *twopc.Coordinator[string] {
				return coordinator.NewPostgresBasicGRPCCoordinator(pool)
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-postgres-noop-gRPC-1",
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

func Test_grpc_sql_transfer_integration(t *testing.T) {
	t.Parallel()
	var sqlProvider = func(pool *pgxpool.Pool) *adapter.GRPCTransferHandler {
		return adapter.NewTransferGRPCHandler(pool)
	}
	tests := []testContainersTestCase[*adapter.GRPCTransferHandler]{
		{
			name: "simple postgres sql gRPC happy path",
			handlersProviders: []handlerProvider[*adapter.GRPCTransferHandler]{
				sqlProvider,
				sqlProvider,
				sqlProvider,
			},
			handlersMapper: transferGRPCServerRequests,
			txCoordinatorProvider: func(pool *pgxpool.Pool) *twopc.Coordinator[string] {
				return coordinator.NewPostgresTransferGRPCCoordinator(pool)
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-postgres-sql-gRPC-1",
				transactions: []transaction{
					{
						participantNumber: 0,
						payload: participant.TransferPayload{
							SenderID:   "Alice",
							ReceiverID: "Bob",
							Amount:     100.5,
						},
					},
					{
						participantNumber: 1,
						payload: participant.TransferPayload{
							SenderID:   "Bob",
							ReceiverID: "Cecile",
							Amount:     100.5,
						},
					},
					{
						participantNumber: 2,
						payload: participant.TransferPayload{
							SenderID:   "Cecile",
							ReceiverID: "Alice",
							Amount:     100.5,
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

func Test_grpc_sql_eventual_consistency_postgres_noop_gRPC(t *testing.T) {
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
		transactionID: "tx-eventual-consistency-postgres-noop-gRPC-1",
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
