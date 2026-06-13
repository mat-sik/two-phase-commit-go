//go:build testcontainers

package test

import (
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/adapter"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func Test_postgres_persistence_transfer_logic_gRPC(t *testing.T) {
	t.Parallel()
	var transferProvider = func(pool *pgxpool.Pool) *adapter.GRPCTransferHandler {
		return adapter.NewTransferGRPCHandler(pool)
	}
	tests := []testContainersTestCase[*adapter.GRPCTransferHandler]{
		{
			name: "happy path",
			handlersConfig: handlersConfig[*adapter.GRPCTransferHandler]{
				providers: []handlerProvider[*adapter.GRPCTransferHandler]{
					transferProvider,
					transferProvider,
					transferProvider,
				},
				mapper: transferGRPCServerRequests,
			},
			coordinatorConfig: testContainersCoordinatorConfig{
				persistenceConfigProvider: coordinator.NewPostgresPersistenceConfig,
				clientConfig: coordinatorClientConfig{
					clientConfigProvider: newClientConfig(coordinator.NewTransferGRPCClient()),
				},
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-postgres-transfer-grpc-1",
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

func Test_postgres_persistence_transfer_logic_REST(t *testing.T) {
	t.Parallel()
	var transferProvider = func(pool *pgxpool.Pool) *http.ServeMux {
		return adapter.NewTransferMux(pool)
	}
	tests := []testContainersTestCase[*http.ServeMux]{
		{
			name: "postgres transfer REST happy path",
			handlersConfig: handlersConfig[*http.ServeMux]{
				providers: []handlerProvider[*http.ServeMux]{
					transferProvider,
					transferProvider,
					transferProvider,
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
				transactionID: "tx-postgres-transfer-rest-1",
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
