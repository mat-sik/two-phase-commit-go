package test

import (
	"testing"

	"github.com/mat-sik/two-phase-commit-go/examples/grpc-unary/internal/client"
	"github.com/mat-sik/two-phase-commit-go/examples/grpc-unary/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func Test_mock_integration(t *testing.T) {
	tests := []testCase{
		{
			name: "simple happy path",
			serverConfigs: []serverConfig{
				{
					handler: client.NewNoopHandler(),
				},
				{
					handler: client.NewNoopHandler(),
				},
				{
					handler: client.NewNoopHandler(),
				},
			},
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewGRPCClient,
			),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-1",
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
			serverConfigs: []serverConfig{
				{
					handler: client.NewFailingNoopHandler(1, 0, 1),
				},
				{
					handler: client.NewFailingNoopHandler(0, 0, 1),
				},
				{
					handler: client.NewFailingNoopHandler(1, 0, 0),
				},
			},
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewGRPCClient,
			),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-1",
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
			serverConfigs: []serverConfig{
				{
					handler: client.NewFailingNoopHandler(0, 1, 0),
				},
				{
					handler: client.NewFailingNoopHandler(0, 1, 0),
				},
				{
					handler: client.NewFailingNoopHandler(0, 1, 0),
				},
			},
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewGRPCClient,
			),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-1",
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
			runTest(t, tt)
		})
	}
}
