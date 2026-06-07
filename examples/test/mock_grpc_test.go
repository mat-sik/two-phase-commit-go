package test

import (
	"testing"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/adapter"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func Test_mock_basic_grpc_integration(t *testing.T) {
	t.Parallel()
	tests := []testCase{
		{
			name: "basic gRPC happy path",
			// TODO: It should be possible to mix gRPC with REST clients
			runServerRequests: basicGRPCServerRequests([]*adapter.GRPCBasicHandler{
				adapter.NewBasicGRPCHandler(),
				adapter.NewBasicGRPCHandler(),
				adapter.NewBasicGRPCHandler(),
			}),
			txCoordinator: coordinator.NewMockBasicGRPCCoordinator(),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-basic-gRPC-1",
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
		{
			name: "failing basic gRPC -> rollback",
			runServerRequests: basicGRPCServerRequests([]*adapter.GRPCBasicHandler{
				adapter.NewFailingBasicGRPCHandler(1, 0, 1),
				adapter.NewFailingBasicGRPCHandler(0, 0, 1),
				adapter.NewFailingBasicGRPCHandler(1, 0, 0),
			}),
			txCoordinator: coordinator.NewMockBasicGRPCCoordinator(),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-basic-gRPC-2",
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
			wantErr:       true,
			wantedOutcome: twopc.OutcomeRolledBack,
		},
		{
			name: "failing basic gRPC -> committed",
			runServerRequests: basicGRPCServerRequests([]*adapter.GRPCBasicHandler{
				adapter.NewFailingBasicGRPCHandler(0, 1, 0),
				adapter.NewFailingBasicGRPCHandler(0, 1, 0),
				adapter.NewFailingBasicGRPCHandler(0, 1, 0),
			}),
			txCoordinator: coordinator.NewMockBasicGRPCCoordinator(),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-basic-gRPC-3",
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
