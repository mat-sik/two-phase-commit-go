package test

import (
	"net/http"
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/basic"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/adapter"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func Test_rest_mock_integration(t *testing.T) {
	t.Parallel()
	tests := []testCase{
		{
			name: "simple mock noop REST happy path",
			runServerRequests: restServerRequests([]*http.ServeMux{
				adapter.NewBasicMux(),
				adapter.NewBasicMux(),
				adapter.NewBasicMux(),
			}),
			txCoordinator: coordinator.NewMockBasicRESTCoordinator(),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-noop-REST-1",
				transactions: []transaction{
					{
						participantNumber: 0,
						payload: basic.RESTPreparePayload{
							Payload:   "one",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 1,
						payload: basic.RESTPreparePayload{
							Payload:   "two",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 2,
						payload: basic.RESTPreparePayload{
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
			runServerRequests: restServerRequests([]*http.ServeMux{
				adapter.NewFailingBasicMux(1, 0, 1),
				adapter.NewFailingBasicMux(0, 0, 1),
				adapter.NewFailingBasicMux(1, 0, 0),
			}),
			txCoordinator: coordinator.NewMockBasicRESTCoordinator(),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-noop-REST-2",
				transactions: []transaction{
					{
						participantNumber: 0,
						payload: basic.RESTPreparePayload{
							Payload:   "one",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 1,
						payload: basic.RESTPreparePayload{
							Payload:   "two",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 2,
						payload: basic.RESTPreparePayload{
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
			runServerRequests: restServerRequests([]*http.ServeMux{
				adapter.NewFailingBasicMux(0, 1, 0),
				adapter.NewFailingBasicMux(0, 1, 0),
				adapter.NewFailingBasicMux(0, 1, 0),
			}),
			txCoordinator: coordinator.NewMockBasicRESTCoordinator(),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-noop-REST-3",
				transactions: []transaction{
					{
						participantNumber: 0,
						payload: basic.RESTPreparePayload{
							Payload:   "one",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 1,
						payload: basic.RESTPreparePayload{
							Payload:   "two",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 2,
						payload: basic.RESTPreparePayload{
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
