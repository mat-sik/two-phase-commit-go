package test

import (
	"testing"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func Test__grpc_mock_integration(t *testing.T) {
	tests := []testCase{
		{
			name: "simple happy path",
			runServerRequests: client.GRPCServerRequests([]*client.GRPCHandler{
				client.NewNoopGRPCHandler(),
				client.NewNoopGRPCHandler(),
				client.NewNoopGRPCHandler(),
			}),
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewGRPCClient,
			),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-grpc-mock-1",
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
			name: "Some failing on prepare some other on rollback, but eventually all rollbacks go through",
			runServerRequests: client.GRPCServerRequests([]*client.GRPCHandler{
				client.NewFailingNoopGRPCHandler(1, 0, 1),
				client.NewFailingNoopGRPCHandler(0, 0, 1),
				client.NewFailingNoopGRPCHandler(1, 0, 0),
			}),
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewGRPCClient,
			),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-grpc-mock-2",
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
			name: "some commits fail, but eventually all commits go through",
			runServerRequests: client.GRPCServerRequests([]*client.GRPCHandler{
				client.NewFailingNoopGRPCHandler(0, 1, 0),
				client.NewFailingNoopGRPCHandler(0, 1, 0),
				client.NewFailingNoopGRPCHandler(0, 1, 0),
			}),
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewGRPCClient,
			),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-grpc-mock-3",
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
