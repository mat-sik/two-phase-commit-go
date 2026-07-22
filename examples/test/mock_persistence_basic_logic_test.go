package test

import (
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/basic"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func Test_mock_persistence_basic_logic(t *testing.T) {
	t.Parallel()
	tests := []testCase{
		{
			name: "gRPC client happy path",
			serverSpecs: []serverSpec{
				restBasicLogicServerSpec{},
				restBasicLogicServerSpec{},
				restBasicLogicServerSpec{},
			},
			coordinatorConfig: coordinatorConfig{
				persistenceConfig: coordinator.NewMockPersistenceConfig(),
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-basic-gRPC-1",
				transactions: []transaction{
					{
						payload:           "one",
						communicationType: communicationTypeRest,
					},
					{
						payload:           "two",
						communicationType: communicationTypeRest,
					},
					{
						payload:           "three",
						communicationType: communicationTypeRest,
					},
				},
			},
			wantErr:       false,
			wantedOutcome: twopc.OutcomeSuccess,
		},
		{
			name: "REST client happy path",
			serverSpecs: []serverSpec{
				restBasicLogicServerSpec{},
				restBasicLogicServerSpec{},
				restBasicLogicServerSpec{},
			},
			coordinatorConfig: coordinatorConfig{
				persistenceConfig: coordinator.NewMockPersistenceConfig(),
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-basic-REST-1",
				transactions: []transaction{
					{
						payload: basic.PreparePayload{
							Payload:   "one",
							CreatedAt: time.Now(),
						},
						communicationType: communicationTypeRest,
					},
					{
						payload: basic.PreparePayload{
							Payload:   "two",
							CreatedAt: time.Now(),
						},
						communicationType: communicationTypeRest,
					},
					{
						payload: basic.PreparePayload{
							Payload:   "three",
							CreatedAt: time.Now(),
						},
						communicationType: communicationTypeRest,
					},
				},
			},
			wantErr:       false,
			wantedOutcome: twopc.OutcomeSuccess,
		},
		{
			name: "mixed clients happy path",
			serverSpecs: []serverSpec{
				gRPCBasicLogicServerSpec{},
				gRPCBasicLogicServerSpec{},
				restBasicLogicServerSpec{},
			},
			coordinatorConfig: coordinatorConfig{
				persistenceConfig: coordinator.NewMockPersistenceConfig(),
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-basic-mixed-1",
				transactions: []transaction{
					{
						payload:           "one",
						communicationType: communicationTypeBasicGRPC,
					},
					{
						payload:           "two",
						communicationType: communicationTypeBasicGRPC,
					},
					{
						payload:           "three",
						communicationType: communicationTypeRest,
					},
				},
			},
			wantErr:       false,
			wantedOutcome: twopc.OutcomeSuccess,
		},
		{
			name: "failing gRPC client -> rollback",
			serverSpecs: []serverSpec{
				gRPCBasicLogicServerSpec{
					prepareFailUntilAttempt:  1,
					commitFailUntilAttempt:   0,
					rollbackFailUntilAttempt: 1,
				},
				gRPCBasicLogicServerSpec{
					prepareFailUntilAttempt:  0,
					commitFailUntilAttempt:   0,
					rollbackFailUntilAttempt: 1,
				},
				gRPCBasicLogicServerSpec{
					prepareFailUntilAttempt:  1,
					commitFailUntilAttempt:   0,
					rollbackFailUntilAttempt: 0,
				},
			},
			coordinatorConfig: coordinatorConfig{
				persistenceConfig: coordinator.NewMockPersistenceConfig(),
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-basic-gRPC-2",
				transactions: []transaction{
					{
						payload:           "one",
						communicationType: communicationTypeBasicGRPC,
					},
					{
						payload:           "two",
						communicationType: communicationTypeBasicGRPC,
					},
					{
						payload:           "three",
						communicationType: communicationTypeBasicGRPC,
					},
				},
			},
			wantErr:       true,
			wantedOutcome: twopc.OutcomeFailed,
		},
		{
			name: "failing REST client -> rollback",
			serverSpecs: []serverSpec{
				restBasicLogicServerSpec{
					prepareFailUntilAttempt:  1,
					commitFailUntilAttempt:   0,
					rollbackFailUntilAttempt: 1,
				},
				restBasicLogicServerSpec{
					prepareFailUntilAttempt:  0,
					commitFailUntilAttempt:   0,
					rollbackFailUntilAttempt: 1,
				},
				restBasicLogicServerSpec{
					prepareFailUntilAttempt:  1,
					commitFailUntilAttempt:   0,
					rollbackFailUntilAttempt: 0,
				},
			},
			coordinatorConfig: coordinatorConfig{
				persistenceConfig: coordinator.NewMockPersistenceConfig(),
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-basic-REST-2",
				transactions: []transaction{
					{
						payload: basic.PreparePayload{
							Payload:   "one",
							CreatedAt: time.Now(),
						},
						communicationType: communicationTypeRest,
					},
					{
						payload: basic.PreparePayload{
							Payload:   "two",
							CreatedAt: time.Now(),
						},
						communicationType: communicationTypeRest,
					},
					{
						payload: basic.PreparePayload{
							Payload:   "three",
							CreatedAt: time.Now(),
						},
						communicationType: communicationTypeRest,
					},
				},
			},
			wantErr:       true,
			wantedOutcome: twopc.OutcomeFailed,
		},
		{
			name: "failing gRPC client -> committed",
			serverSpecs: []serverSpec{
				gRPCBasicLogicServerSpec{
					prepareFailUntilAttempt:  0,
					commitFailUntilAttempt:   1,
					rollbackFailUntilAttempt: 0,
				},
				gRPCBasicLogicServerSpec{
					prepareFailUntilAttempt:  0,
					commitFailUntilAttempt:   1,
					rollbackFailUntilAttempt: 0,
				},
				gRPCBasicLogicServerSpec{
					prepareFailUntilAttempt:  0,
					commitFailUntilAttempt:   1,
					rollbackFailUntilAttempt: 0,
				},
			},
			coordinatorConfig: coordinatorConfig{
				persistenceConfig: coordinator.NewMockPersistenceConfig(),
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-basic-gRPC-3",
				transactions: []transaction{
					{
						payload:           "one",
						communicationType: communicationTypeBasicGRPC,
					},
					{
						payload:           "two",
						communicationType: communicationTypeBasicGRPC,
					},
					{
						payload:           "three",
						communicationType: communicationTypeBasicGRPC,
					},
				},
			},
			wantErr:       true,
			wantedOutcome: twopc.OutcomeSuccess,
		},
		{
			name: "failing REST client -> committed",
			serverSpecs: []serverSpec{
				restBasicLogicServerSpec{
					prepareFailUntilAttempt:  0,
					commitFailUntilAttempt:   1,
					rollbackFailUntilAttempt: 0,
				},
				restBasicLogicServerSpec{
					prepareFailUntilAttempt:  0,
					commitFailUntilAttempt:   1,
					rollbackFailUntilAttempt: 0,
				},
				restBasicLogicServerSpec{
					prepareFailUntilAttempt:  0,
					commitFailUntilAttempt:   1,
					rollbackFailUntilAttempt: 0,
				},
			},
			coordinatorConfig: coordinatorConfig{
				persistenceConfig: coordinator.NewMockPersistenceConfig(),
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-basic-REST-3",
				transactions: []transaction{
					{
						payload: basic.PreparePayload{
							Payload:   "one",
							CreatedAt: time.Now(),
						},
						communicationType: communicationTypeRest,
					},
					{
						payload: basic.PreparePayload{
							Payload:   "two",
							CreatedAt: time.Now(),
						},
						communicationType: communicationTypeRest,
					},
					{
						payload: basic.PreparePayload{
							Payload:   "three",
							CreatedAt: time.Now(),
						},
						communicationType: communicationTypeRest,
					},
				},
			},
			wantErr:       true,
			wantedOutcome: twopc.OutcomeSuccess,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runTest(t, tt)
		})
	}
}
