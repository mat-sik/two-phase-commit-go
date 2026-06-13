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

func Test_mock_persistence_basic_logic(t *testing.T) {
	t.Parallel()
	tests := []testCase{
		{
			name: "gRPC client happy path",
			runServerRequests: basicGRPCServerRequests([]*adapter.GRPCBasicHandler{
				adapter.NewBasicGRPCHandler(),
				adapter.NewBasicGRPCHandler(),
				adapter.NewBasicGRPCHandler(),
			}),
			coordinatorConfig: coordinatorConfig{
				persistenceConfig: coordinator.NewMockPersistenceConfig(),
				clientConfig: coordinatorClientConfig{
					clientConfigProvider: newClientConfig(coordinator.NewBasicGRPCClient()),
				},
			},
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
			name: "REST client happy path",
			runServerRequests: restServerRequests([]*http.ServeMux{
				adapter.NewBasicMux(),
				adapter.NewBasicMux(),
				adapter.NewBasicMux(),
			}),
			coordinatorConfig: coordinatorConfig{
				persistenceConfig: coordinator.NewMockPersistenceConfig(),
				clientConfig: coordinatorClientConfig{
					clientConfigProvider: newClientConfig(coordinator.NewRestClient()),
				},
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-basic-REST-1",
				transactions: []transaction{
					{
						participantNumber: 0,
						payload: basic.PreparePayload{
							Payload:   "one",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 1,
						payload: basic.PreparePayload{
							Payload:   "two",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 2,
						payload: basic.PreparePayload{
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
			name: "mixed clients happy path",
			runServerRequests: []runServerRequest{
				mapFromGRPCBasicHandler(adapter.NewBasicGRPCHandler()),
				mapFromGRPCBasicHandler(adapter.NewBasicGRPCHandler()),
				mapFromMux(adapter.NewBasicMux()),
			},
			coordinatorConfig: coordinatorConfig{
				persistenceConfig: coordinator.NewMockPersistenceConfig(),
				clientConfig: coordinatorClientConfig{
					clientConfigProvider: newMixedClientConfig(),
					participantTransports: map[int]transportType{
						0: transportTypeBasicGRPC,
						1: transportTypeBasicGRPC,
						2: transportTypeREST,
					},
				},
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-basic-mixed-1",
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
			name: "failing gRPC client -> rollback",
			runServerRequests: basicGRPCServerRequests([]*adapter.GRPCBasicHandler{
				adapter.NewFailingBasicGRPCHandler(1, 0, 1),
				adapter.NewFailingBasicGRPCHandler(0, 0, 1),
				adapter.NewFailingBasicGRPCHandler(1, 0, 0),
			}),
			coordinatorConfig: coordinatorConfig{
				persistenceConfig: coordinator.NewMockPersistenceConfig(),
				clientConfig: coordinatorClientConfig{
					clientConfigProvider: newClientConfig(coordinator.NewBasicGRPCClient()),
				},
			},
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
			name: "failing REST client -> rollback",
			runServerRequests: restServerRequests([]*http.ServeMux{
				adapter.NewFailingBasicMux(1, 0, 1),
				adapter.NewFailingBasicMux(0, 0, 1),
				adapter.NewFailingBasicMux(1, 0, 0),
			}),
			coordinatorConfig: coordinatorConfig{
				persistenceConfig: coordinator.NewMockPersistenceConfig(),
				clientConfig: coordinatorClientConfig{
					clientConfigProvider: newClientConfig(coordinator.NewRestClient()),
				},
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-basic-REST-2",
				transactions: []transaction{
					{
						participantNumber: 0,
						payload: basic.PreparePayload{
							Payload:   "one",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 1,
						payload: basic.PreparePayload{
							Payload:   "two",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 2,
						payload: basic.PreparePayload{
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
			name: "failing gRPC client -> committed",
			runServerRequests: basicGRPCServerRequests([]*adapter.GRPCBasicHandler{
				adapter.NewFailingBasicGRPCHandler(0, 1, 0),
				adapter.NewFailingBasicGRPCHandler(0, 1, 0),
				adapter.NewFailingBasicGRPCHandler(0, 1, 0),
			}),
			coordinatorConfig: coordinatorConfig{
				persistenceConfig: coordinator.NewMockPersistenceConfig(),
				clientConfig: coordinatorClientConfig{
					clientConfigProvider: newClientConfig(coordinator.NewBasicGRPCClient()),
				},
			},
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
		{
			name: "failing REST client -> committed",
			runServerRequests: restServerRequests([]*http.ServeMux{
				adapter.NewFailingBasicMux(0, 1, 0),
				adapter.NewFailingBasicMux(0, 1, 0),
				adapter.NewFailingBasicMux(0, 1, 0),
			}),
			coordinatorConfig: coordinatorConfig{
				persistenceConfig: coordinator.NewMockPersistenceConfig(),
				clientConfig: coordinatorClientConfig{
					clientConfigProvider: newClientConfig(coordinator.NewRestClient()),
				},
			},
			distributedTransaction: distributedTransaction{
				transactionID: "tx-mock-basic-REST-3",
				transactions: []transaction{
					{
						participantNumber: 0,
						payload: basic.PreparePayload{
							Payload:   "one",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 1,
						payload: basic.PreparePayload{
							Payload:   "two",
							CreatedAt: time.Now(),
						},
					},
					{
						participantNumber: 2,
						payload: basic.PreparePayload{
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
