//go:build testcontainers

package test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func Test_grpc_sql_integration(t *testing.T) {
	tests := []testContainersTestCase[*client.GRPCBasicHandler]{
		{
			name: "simple postgres noop gRPC happy path",
			handlers: []*client.GRPCBasicHandler{
				client.NewNoopGRPCHandler(),
				client.NewNoopGRPCHandler(),
				client.NewNoopGRPCHandler(),
			},
			handlersMapper: client.GRPCServerRequests,
			txCoordinatorProvider: func(pool *pgxpool.Pool) *twopc.Coordinator[string] {
				return coordinator.NewSQLGRPCCoordinator(pool)
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
		//{
		//	name: "simple postgres sql gRPC happy path",
		//	handlersProviders: []handlerProvider[*client.GRPCHandler]{
		//		{
		//			providerFunc: func(pool *pgxpool.Pool) *client.GRPCHandler {
		//				return client.NewSQLGRPCHandler(pool)
		//			},
		//		},
		//		{
		//			providerFunc: func(pool *pgxpool.Pool) *client.GRPCHandler {
		//				return client.NewSQLGRPCHandler(pool)
		//			},
		//		},
		//		{
		//			providerFunc: func(pool *pgxpool.Pool) *client.GRPCHandler {
		//				return client.NewSQLGRPCHandler(pool)
		//			},
		//		},
		//	},
		//	handlersMapper: client.GRPCServerRequests,
		//	txCoordinatorProvider: func(pool *pgxpool.Pool) *twopc.Coordinator[string] {
		//		return coordinator.NewSQLGRPCCoordinator(pool)
		//	},
		//	distributedTransaction: distributedTransaction{
		//		transactionID: "tx-postgres-sql-gRPC-1",
		//		transactions: []transaction{
		//			{
		//				participantNumber: 0,
		//				payload: client.TransferPayload{
		//					SenderID:   "Alice",
		//					ReceiverID: "Bob",
		//					Amount:     100.5,
		//				},
		//			},
		//			{
		//				participantNumber: 1,
		//				payload: client.TransferPayload{
		//					SenderID:   "Bob",
		//					ReceiverID: "Cecile",
		//					Amount:     100.5,
		//				},
		//			},
		//			{
		//				participantNumber: 2,
		//				payload: client.TransferPayload{
		//					SenderID:   "Cecile",
		//					ReceiverID: "Alice",
		//					Amount:     100.5,
		//				},
		//			},
		//		},
		//	},
		//	wantErr:       false,
		//	wantedOutcome: twopc.OutcomeCommitted,
		//},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runTestContainersTest(t, tt)
		})
	}
}

func Test_grpc_sql_eventual_consistency(t *testing.T) {
	t.Run("eventual consistency postgres noop gRPC", func(t *testing.T) {
		t.Parallel()

		coordinatorPool, coordinatorDbDropper := createCoordinatorDb(t.Context(), t.Name())
		t.Cleanup(coordinatorDbDropper)

		txCoordinator := coordinator.NewSQLGRPCCoordinator(
			coordinatorPool,
			twopc.WithBackoffMax(200*time.Millisecond),
		)

		tx := distributedTransaction{
			transactionID: "tx-eventual-consistency-postgres-noop-gRPC-1",
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
		}

		srvConfig := client.GRPCServerRequests([]*client.GRPCBasicHandler{
			client.NewFailingNoopGRPCHandler(0, 15, 0),
			client.NewFailingNoopGRPCHandler(0, 20, 0),
			client.NewFailingNoopGRPCHandler(0, 30, 0),
		})

		srvBundle, err := client.RunServers(srvConfig)
		if err != nil {
			t.Fatalf("failed to start servers: %v", err)
		}

		testCtx, testCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer testCancel()
		for {
			if err = testCtx.Err(); err != nil {
				t.Fatalf("failed to eventually finish in committed state, err: %v", err)
			}

			ctx, cancel := context.WithTimeout(testCtx, 1*time.Second)

			addresses := srvBundle.Addresses()
			outcome := txCoordinator.Execute(ctx, tx.toTwopc(addresses))
			cancel()
			if outcome.Outcome() == twopc.OutcomeCommitted {
				break
			}
		}

		errs := srvBundle.Shutdown()
		if len(errs) != 0 {
			t.Errorf("got %d server errors: %v", len(errs), errs)
		}
	})
}
