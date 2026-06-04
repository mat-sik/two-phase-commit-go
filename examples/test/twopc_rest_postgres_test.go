package test

import (
	"net/http"
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func Test_sql_rest_integration(t *testing.T) {
	tests := []testCase{
		{
			name: "simple REST happy path",
			runServerRequests: client.RESTServerRequests([]*http.ServeMux{
				client.NewNoopMux(),
				client.NewNoopMux(),
				client.NewNoopMux(),
			}),
			txCoordinator: coordinator.NewSQLRESTCoordinator(pool),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-rest-psql-1",
				transactions: []transaction{
					{
						participantNumber: 0,
						payload: coordinator.RESTPreparePayload{
							Payload:   "one",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 1,
						payload: coordinator.RESTPreparePayload{
							Payload:   "two",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 2,
						payload: coordinator.RESTPreparePayload{
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
			t.Cleanup(cleanup)
			runTest(t, tt)
		})
	}
}
