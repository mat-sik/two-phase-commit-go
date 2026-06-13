//go:build testcontainers

package test

import (
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/basic"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/adapter"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func Test_postgres_basic_rest_integration(t *testing.T) {
	t.Parallel()
	tests := []testContainersTestCase[*http.ServeMux]{
		{
			name: "postgres basic REST happy path",
			handlers: []*http.ServeMux{
				adapter.NewBasicMux(),
				adapter.NewBasicMux(),
				adapter.NewBasicMux(),
			},
			handlersMapper: restServerRequests,
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

func Test_postgres_transfer_rest_integration(t *testing.T) {
	t.Parallel()
	var transferProvider = func(pool *pgxpool.Pool) *http.ServeMux {
		return adapter.NewTransferMux(pool)
	}
	tests := []testContainersTestCase[*http.ServeMux]{
		{
			name: "postgres transfer REST happy path",
			handlersProviders: []handlerProvider[*http.ServeMux]{
				transferProvider,
				transferProvider,
				transferProvider,
			},
			handlersMapper: restServerRequests,
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
