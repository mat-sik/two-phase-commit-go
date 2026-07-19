//go:build testcontainers

package test

import (
	"context"
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/basic"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/adapter"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func Test_postgres_persistence(t *testing.T) {
	t.Parallel()
	tests := []testContainersTestCase{
		{
			name: "gRPC client basic logic happy path",
			serverSpecs: []serverSpec{
				gRPCBasicLogicServerSpec{},
				gRPCBasicLogicServerSpec{},
				gRPCBasicLogicServerSpec{},
			},
			coordinatorConfig: testContainersCoordinatorConfig{
				persistenceConfigProvider: coordinator.NewPostgresPersistenceConfig,
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-postgres-basic-grpc-1",
				transactions: []transaction{
					{
						payload:           "one",
						communicationType: communicationTypeBasicGRPC,
					},
					{
						payload:           "two",
						communicationType: communicationTypeBasicGRPC,
					},
					{
						payload:           "three",
						communicationType: communicationTypeBasicGRPC,
					},
				},
			},
			wantErr:       false,
			wantedOutcome: twopc.OutcomeSuccess,
		},
		{
			name: "REST client basic logic happy path",
			serverSpecs: []serverSpec{
				restBasicLogicServerSpec{},
				restBasicLogicServerSpec{},
				restBasicLogicServerSpec{},
			},
			coordinatorConfig: testContainersCoordinatorConfig{
				persistenceConfigProvider: coordinator.NewPostgresPersistenceConfig,
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-postgres-basic-REST-1",
				transactions: []transaction{
					{
						payload: basic.PreparePayload{
							Payload:   "one",
							CreatedAt: time.Now(),
						},
						communicationType: communicationTypeRest,
					},
					{
						payload: basic.PreparePayload{
							Payload:   "two",
							CreatedAt: time.Now(),
						},
						communicationType: communicationTypeRest,
					},
					{
						payload: basic.PreparePayload{
							Payload:   "three",
							CreatedAt: time.Now(),
						},
						communicationType: communicationTypeRest,
					},
				},
			},
			wantErr:       false,
			wantedOutcome: twopc.OutcomeSuccess,
		},
		{
			name: "gRPC client transfer logic happy path",
			serverSpecs: []serverSpec{
				&gRPCTransferLogicServerSpec{},
				&gRPCTransferLogicServerSpec{},
				&gRPCTransferLogicServerSpec{},
			},
			coordinatorConfig: testContainersCoordinatorConfig{
				persistenceConfigProvider: coordinator.NewPostgresPersistenceConfig,
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-postgres-transfer-grpc-1",
				transactions: []transaction{
					{
						payload: participant.TransferPayload{
							SenderID:   "Alice",
							ReceiverID: "Bob",
							Amount:     100.5,
						},
						communicationType: communicationTypeTransferGrpc,
					},
					{
						payload: participant.TransferPayload{
							SenderID:   "Bob",
							ReceiverID: "Cecile",
							Amount:     100.5,
						},
						communicationType: communicationTypeTransferGrpc,
					},
					{
						payload: participant.TransferPayload{
							SenderID:   "Cecile",
							ReceiverID: "Alice",
							Amount:     100.5,
						},
						communicationType: communicationTypeTransferGrpc,
					},
				},
			},
			wantErr:       false,
			wantedOutcome: twopc.OutcomeSuccess,
		},
		{
			name: "REST client transfer logic happy path",
			serverSpecs: []serverSpec{
				&restTransferLogicServerSpec{},
				&restTransferLogicServerSpec{},
				&restTransferLogicServerSpec{},
			},
			coordinatorConfig: testContainersCoordinatorConfig{
				persistenceConfigProvider: coordinator.NewPostgresPersistenceConfig,
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-postgres-transfer-rest-1",
				transactions: []transaction{
					{
						payload: participant.TransferPayload{
							SenderID:   "Alice",
							ReceiverID: "Bob",
							Amount:     100.5,
						},
						communicationType: communicationTypeRest,
					},
					{
						payload: participant.TransferPayload{
							SenderID:   "Bob",
							ReceiverID: "Cecile",
							Amount:     100.5,
						},
						communicationType: communicationTypeRest,
					},
					{
						payload: participant.TransferPayload{
							SenderID:   "Cecile",
							ReceiverID: "Alice",
							Amount:     100.5,
						},
						communicationType: communicationTypeRest,
					},
				},
			},
			wantErr:       false,
			wantedOutcome: twopc.OutcomeSuccess,
		},
		{
			name: "mixed client basic logic happy path",
			serverSpecs: []serverSpec{
				gRPCBasicLogicServerSpec{},
				gRPCBasicLogicServerSpec{},
				restBasicLogicServerSpec{},
			},
			coordinatorConfig: testContainersCoordinatorConfig{
				persistenceConfigProvider: coordinator.NewPostgresPersistenceConfig,
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-postgres-basic-mixed-1",
				transactions: []transaction{
					{
						payload:           "one",
						communicationType: communicationTypeBasicGRPC,
					},
					{
						payload:           "two",
						communicationType: communicationTypeBasicGRPC,
					},
					{
						payload:           "three",
						communicationType: communicationTypeRest,
					},
				},
			},
			wantErr:       false,
			wantedOutcome: twopc.OutcomeSuccess,
		},
		{
			name: "mixed client mixed logic happy path",
			serverSpecs: []serverSpec{
				gRPCBasicLogicServerSpec{},
				&restTransferLogicServerSpec{},
				restBasicLogicServerSpec{},
			},
			coordinatorConfig: testContainersCoordinatorConfig{
				persistenceConfigProvider: coordinator.NewPostgresPersistenceConfig,
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-postgres-mixed-mixed-1",
				transactions: []transaction{
					{
						payload:           "one",
						communicationType: communicationTypeBasicGRPC,
					},
					{
						payload: participant.TransferPayload{
							SenderID:   "Bob",
							ReceiverID: "Cecile",
							Amount:     100.5,
						},
						communicationType: communicationTypeRest,
					},
					{
						payload:           "three",
						communicationType: communicationTypeRest,
					},
				},
			},
			wantErr:       false,
			wantedOutcome: twopc.OutcomeSuccess,
		},
		{
			name: "mixed client mixed logic fail path because non matching server",
			serverSpecs: []serverSpec{
				gRPCBasicLogicServerSpec{},
				&restTransferLogicServerSpec{},
				restBasicLogicServerSpec{},
			},
			coordinatorConfig: testContainersCoordinatorConfig{
				persistenceConfigProvider: coordinator.NewPostgresPersistenceConfig,
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-postgres-mixed-mixed-1",
				transactions: []transaction{
					{
						payload:           "one",
						communicationType: communicationTypeBasicGRPC,
					},
					{
						payload: participant.TransferPayload{
							SenderID:   "Bob",
							ReceiverID: "Cecile",
							Amount:     100.5,
						},
						communicationType: communicationTypeTransferGrpc,
					},
					{
						payload:           "three",
						communicationType: communicationTypeRest,
					},
				},
			},
			wantErr:       true,
			wantedOutcome: twopc.OutcomeFailed,
		},
		{
			name: "mixed client mixed logic fail path because one server is not running",
			serverSpecs: []serverSpec{
				gRPCBasicLogicServerSpec{},
				noopServerSpec{},
				restBasicLogicServerSpec{},
			},
			coordinatorConfig: testContainersCoordinatorConfig{
				persistenceConfigProvider: coordinator.NewPostgresPersistenceConfig,
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-postgres-mixed-mixed-1",
				transactions: []transaction{
					{
						payload:           "one",
						communicationType: communicationTypeBasicGRPC,
					},
					{
						payload: participant.TransferPayload{
							SenderID:   "Bob",
							ReceiverID: "Cecile",
							Amount:     100.5,
						},
						communicationType: communicationTypeTransferGrpc,
					},
					{
						payload:           "three",
						communicationType: communicationTypeRest,
					},
				},
			},
			wantErr:       true,
			wantedOutcome: twopc.OutcomeFailed,
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

	txCoordinator := twopc.NewCoordinator(
		coordinator.NewPostgresPersistenceConfig(coordinatorPool),
		coordinator.NewBasicGRPCClient(),
		twopc.WithBackoffMax(200*time.Millisecond),
	)
	tx := distributedTransaction{
		transactionID: "tx-postgres-basic-grpc-eventual-consistency-1",
		transactions: []transaction{
			{
				payload:           "one",
				communicationType: communicationTypeBasicGRPC,
			},
			{
				payload:           "two",
				communicationType: communicationTypeBasicGRPC,
			},
			{
				payload:           "three",
				communicationType: communicationTypeBasicGRPC,
			},
		},
	}
	srvConfig := basicGRPCServerLaunches([]*adapter.GRPCBasicHandler{
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
		if outcome.Outcome() == twopc.OutcomeSuccess {
			break
		}
	}
}
