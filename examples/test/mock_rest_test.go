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
			name: "simple mock noop REST happy path",
			runServerRequests: client.RESTServerRequests([]*http.ServeMux{
				client.NewNoopMux(),
				client.NewNoopMux(),
				client.NewNoopMux(),
			}),
			txCoordinator: coordinator.NewMockRESTCoordinator(),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-noop-REST-1",
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
			name: "failing mock noop REST -> rollback",
			runServerRequests: client.RESTServerRequests([]*http.ServeMux{
				client.NewFailingNoopMux(1, 0, 1),
				client.NewFailingNoopMux(0, 0, 1),
				client.NewFailingNoopMux(1, 0, 0),
			}),
			txCoordinator: coordinator.NewMockRESTCoordinator(),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-noop-REST-2",
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
			name: "failing mock noop REST -> committed",
			runServerRequests: client.RESTServerRequests([]*http.ServeMux{
				client.NewFailingNoopMux(0, 1, 0),
				client.NewFailingNoopMux(0, 1, 0),
				client.NewFailingNoopMux(0, 1, 0),
			}),
			txCoordinator: coordinator.NewMockRESTCoordinator(),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-noop-REST-3",
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
			runTest(t, tt)
		})
	}
}
