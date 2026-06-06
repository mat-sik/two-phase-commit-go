package test

import (
	"testing"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func Test_grpc_mock_integration(t *testing.T) {
	tests := []testCase{
		{
			name: "simple mock noop gRPC happy path",
			// TODO: It should be possible to mix gRPC with REST clients
			runServerRequests: client.BasicGRPCServerRequests([]*client.GRPCBasicHandler{
				client.NewNoopGRPCHandler(),
				client.NewNoopGRPCHandler(),
				client.NewNoopGRPCHandler(),
			}),
			txCoordinator: coordinator.NewMockGRPCCoordinator(),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-noop-gRPC-1",
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
			name: "failing mock noop gRPC -> rollback",
			runServerRequests: client.BasicGRPCServerRequests([]*client.GRPCBasicHandler{
				client.NewFailingNoopGRPCHandler(1, 0, 1),
				client.NewFailingNoopGRPCHandler(0, 0, 1),
				client.NewFailingNoopGRPCHandler(1, 0, 0),
			}),
			txCoordinator: coordinator.NewMockGRPCCoordinator(),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-noop-gRPC-2",
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
			name: "failing mock noop gRPC -> committed",
			runServerRequests: client.BasicGRPCServerRequests([]*client.GRPCBasicHandler{
				client.NewFailingNoopGRPCHandler(0, 1, 0),
				client.NewFailingNoopGRPCHandler(0, 1, 0),
				client.NewFailingNoopGRPCHandler(0, 1, 0),
			}),
			txCoordinator: coordinator.NewMockGRPCCoordinator(),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-noop-gRPC-3",
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
