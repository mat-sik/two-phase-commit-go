package test

import (
	"fmt"
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
					port:    30050,
					handler: client.NewNoopHandler(),
				},
				{
					port:    30051,
					handler: client.NewNoopHandler(),
				},
				{
					port:    30052,
					handler: client.NewNoopHandler(),
				},
			},
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewGRPCClient,
			),
			request: twopc.DistributedTransaction[string]{
				TransactionID: "tx-1",
				Transactions: []twopc.Transaction[string]{
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30050),
						Payload:       "one",
					},
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30051),
						Payload:       "two",
					},
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30052),
						Payload:       "three",
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
					port:    30050,
					handler: client.NewFailingNoopHandler(1, 0, 1),
				},
				{
					port:    30051,
					handler: client.NewFailingNoopHandler(0, 0, 1),
				},
				{
					port:    30052,
					handler: client.NewFailingNoopHandler(1, 0, 0),
				},
			},
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewGRPCClient,
			),
			request: twopc.DistributedTransaction[string]{
				TransactionID: "tx-1",
				Transactions: []twopc.Transaction[string]{
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30050),
						Payload:       "one",
					},
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30051),
						Payload:       "two",
					},
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30052),
						Payload:       "three",
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
					port:    30050,
					handler: client.NewFailingNoopHandler(0, 1, 0),
				},
				{
					port:    30051,
					handler: client.NewFailingNoopHandler(0, 1, 0),
				},
				{
					port:    30052,
					handler: client.NewFailingNoopHandler(0, 1, 0),
				},
			},
			txCoordinator: twopc.NewCoordinator(
				coordinator.MockTransactionStateChecker{},
				coordinator.MockStatePersister{},
				coordinator.NewGRPCClient,
			),
			request: twopc.DistributedTransaction[string]{
				TransactionID: "tx-1",
				Transactions: []twopc.Transaction[string]{
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30050),
						Payload:       "one",
					},
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30051),
						Payload:       "two",
					},
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30052),
						Payload:       "three",
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
