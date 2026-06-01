package test

import (
	"net/http"
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func Test_rest_mock_integration(t *testing.T) {
	tests := []testCase{
		{
			name: "simple happy path",
			runServerRequests: client.RESTServerRequests([]*http.ServeMux{
				client.NewNoopMux(),
				client.NewNoopMux(),
				client.NewNoopMux(),
			}),
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewRESTClient,
			),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-rest-mock-1",
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
		{
			name: "Some failing on prepare some other on rollback, but eventually all rollbacks go through",
			runServerRequests: client.RESTServerRequests([]*http.ServeMux{
				client.NewFailingNoopMux(1, 0, 1),
				client.NewFailingNoopMux(0, 0, 1),
				client.NewFailingNoopMux(1, 0, 0),
			}),
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewRESTClient,
			),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-rest-mock-2",
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
			wantErr:       true,
			wantedOutcome: twopc.OutcomeRolledBack,
		},
		{
			name: "some commits fail, but eventually all commits go through",
			runServerRequests: client.RESTServerRequests([]*http.ServeMux{
				client.NewFailingNoopMux(0, 1, 0),
				client.NewFailingNoopMux(0, 1, 0),
				client.NewFailingNoopMux(0, 1, 0),
			}),
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewRESTClient,
			),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-rest-mock-3",
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
			wantErr:       true,
			wantedOutcome: twopc.OutcomeCommitted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runTest(t, tt)
		})
	}
}
