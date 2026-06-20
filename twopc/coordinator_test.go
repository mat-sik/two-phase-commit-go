package twopc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestCoordinator_Execute(t *testing.T) {
	type fields struct {
		persistenceConfig PersistenceConfig[string]
		clientConfig      ClientConfig[string]
	}
	type args struct {
		ctxFunc                func() context.Context
		distributedTransaction DistributedTransaction[string]
	}

	errPrepare := errors.New("prepare failed")
	errCommit := errors.New("commit failed")
	errRollback := errors.New("rollback failed")
	errPersist := errors.New("persist failed")
	errNewClient := errors.New("create new client failed")
	errStateLoad := errors.New("state load failed")

	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErrs    []error
		wantOutcome Outcome
	}{
		// no coordinator failures
		{
			name: "host-a: not started -> prepared -> committed",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "(host-a: not started -> prepared -> committed, host-b: not started -> prepared -> committed) = committed, host-a needs init",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-b": &stubClient{},
					},
					NewClientFunc: newStubClientFunc(map[string]Client{
						"host-a": &stubClient{},
					}, errNewClient),
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "(host-a: not started -> prepared -> rolled back, host-b: not-started -> prepare failed -> rolled back) = rolled back",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
						"host-b": &stubClient{prepareErr: errPrepare},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errPrepare},
			wantOutcome: OutcomeRolledBack,
		},
		{
			name: "(host-a: not started -> prepare failed -> rolled back, host-b: not-started -> prepare failed -> rolled back) = rolled back",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{prepareErr: errPrepare},
						"host-b": &stubClient{prepareErr: errPrepare},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errPrepare},
			wantOutcome: OutcomeRolledBack,
		},
		// retry from coordinator second phase failure
		{
			name: "(host-a: committed -> committed) -> committed",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionCommitted,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "(host-a: prepared -> committed, host-b: prepared -> committed) -> committed",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepared,
							"host-b": TransactionPrepared,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
						"host-b": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "(host-a: prepared -> committed, host-b: committed -> committed) -> committed",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepared,
							"host-b": TransactionCommitted,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
						"host-b": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "(host-a: prepare failed -> rolled back, host-b: prepare failed -> rolled back) -> rolled back",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepareFailed,
							"host-b": TransactionPrepareFailed,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
						"host-b": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeRolledBack,
		},
		{
			name: "(host-a: prepared -> rolled back, host-b: prepare failed -> rolled back) -> rolled back",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepared,
							"host-b": TransactionPrepareFailed,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
						"host-b": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeRolledBack,
		},
		{
			name: "(host-a: prepared -> rolled back, host-b: rolled back -> rolled back) -> rolled back",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepared,
							"host-b": TransactionRolledBack,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
						"host-b": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeRolledBack,
		},
		{
			name: "(host-a: prepare failed -> rolled back, host-b: rolled back -> rolled back) -> rolled back",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepareFailed,
							"host-b": TransactionRolledBack,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
						"host-b": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeRolledBack,
		},
		{
			name: "(host-a: prepared -> rolled back, host-b: prepare failed -> rolled back, host-c: rolled back -> rolled back) -> rolled back",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepared,
							"host-b": TransactionPrepareFailed,
							"host-c": TransactionRolledBack,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
						"host-b": &stubClient{},
						"host-c": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
						{ParticipantID: "host-c", Payload: "p3"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeRolledBack,
		},
		{
			name: "host-a: rolled back -> rolled back",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionRolledBack,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeRolledBack,
		},
		// second phase failures
		{
			name: "(host-a: prepared -> prepared) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepared,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{commitErr: errCommit},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    []error{errCommit},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: prepared -> prepared, host-b: prepared -> prepared) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepared,
							"host-b": TransactionPrepared,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{commitErr: errCommit},
						"host-b": &stubClient{commitErr: errCommit},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errCommit},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: prepared -> prepared, host-b: prepared -> committed) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepared,
							"host-b": TransactionPrepared,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{commitErr: errCommit},
						"host-b": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errCommit},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: prepared -> prepared, host-b: prepared -> committed, host-c: committed -> committed) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepared,
							"host-b": TransactionPrepared,
							"host-c": TransactionCommitted,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{commitErr: errCommit},
						"host-b": &stubClient{},
						"host-c": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
						{ParticipantID: "host-c", Payload: "p3"},
					},
				},
			},
			wantErrs:    []error{errCommit},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: prepare failed -> prepare failed) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepareFailed,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{rollbackErr: errRollback},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    []error{errRollback},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: prepare failed -> prepare failed, host-b: prepare failed -> prepare failed) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepareFailed,
							"host-b": TransactionPrepareFailed,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{rollbackErr: errRollback},
						"host-b": &stubClient{rollbackErr: errRollback},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errRollback},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: prepare failed -> prepare failed, host-b: prepare failed -> rolled back) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepareFailed,
							"host-b": TransactionPrepareFailed,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{rollbackErr: errRollback},
						"host-b": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errRollback},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: prepare failed -> prepare failed, host-b: prepare failed -> rolled back, host-c: rolled back -> rolled back) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepareFailed,
							"host-b": TransactionPrepareFailed,
							"host-c": TransactionRolledBack,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{rollbackErr: errRollback},
						"host-b": &stubClient{},
						"host-c": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
						{ParticipantID: "host-c", Payload: "p3"},
					},
				},
			},
			wantErrs:    []error{errRollback},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: prepare failed -> prepare failed, host-b: prepare -> rolled back) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepareFailed,
							"host-b": TransactionPrepared,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{rollbackErr: errRollback},
						"host-b": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errRollback},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: prepare failed -> prepare failed, host-b: prepare -> rolled back, host-c: rolled back -> rolled back) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepareFailed,
							"host-b": TransactionPrepared,
							"host-c": TransactionRolledBack,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{rollbackErr: errRollback},
						"host-b": &stubClient{},
						"host-c": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
						{ParticipantID: "host-c", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errRollback},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: prepare failed -> prepare failed, host-b: prepared -> rolled back, host-c: prepare failed -> rolled back, host-d: rolled back -> rolled back) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepareFailed,
							"host-b": TransactionPrepared,
							"host-c": TransactionPrepareFailed,
							"host-d": TransactionRolledBack,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{rollbackErr: errRollback},
						"host-b": &stubClient{},
						"host-c": &stubClient{},
						"host-d": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
						{ParticipantID: "host-c", Payload: "p3"},
						{ParticipantID: "host-d", Payload: "p4"},
					},
				},
			},
			wantErrs:    []error{errRollback},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: prepare failed -> prepare failed, host-b: prepare -> prepare failed) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepareFailed,
							"host-b": TransactionPrepared,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{rollbackErr: errRollback},
						"host-b": &stubClient{rollbackErr: errRollback},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errRollback},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: prepare failed -> prepare failed, host-b: prepare -> prepare failed, host-c: prepare failed -> rolled back) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepareFailed,
							"host-b": TransactionPrepared,
							"host-c": TransactionPrepareFailed,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{rollbackErr: errRollback},
						"host-b": &stubClient{rollbackErr: errRollback},
						"host-c": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
						{ParticipantID: "host-c", Payload: "p3"},
					},
				},
			},
			wantErrs:    []error{errRollback},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: prepare failed -> prepare failed, host-b: prepare -> prepare failed, host-c: prepare failed -> rolled back, host-d: rolled back -> rolled back) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-a": TransactionPrepareFailed,
							"host-b": TransactionPrepared,
							"host-c": TransactionPrepareFailed,
							"host-d": TransactionRolledBack,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{rollbackErr: errRollback},
						"host-b": &stubClient{rollbackErr: errRollback},
						"host-c": &stubClient{},
						"host-d": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
						{ParticipantID: "host-c", Payload: "p3"},
						{ParticipantID: "host-d", Payload: "p4"},
					},
				},
			},
			wantErrs:    []error{errRollback},
			wantOutcome: OutcomeInconsistent,
		},
		// reaching second phase failures
		{
			name: "(host-a: not started -> prepared) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{commitErr: errCommit},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    []error{errCommit},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: not started -> prepared, host-b: not started -> prepared) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{commitErr: errCommit},
						"host-b": &stubClient{commitErr: errCommit},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errCommit},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: not started -> prepare failed) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{rollbackErr: errRollback},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    []error{errRollback},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: not started -> prepare failed, host-b: not started -> prepare failed) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{rollbackErr: errRollback},
						"host-b": &stubClient{rollbackErr: errRollback},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errRollback},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "(host-a: not started -> prepared, host-b: not started -> prepare failed) -> inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
						"host-b": &stubClient{prepareErr: errPrepare},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errPrepare},
			wantOutcome: OutcomeInconsistent,
		},
		// retry from coordinator first phase failure
		{
			name: "(host-a: not started -> committed) -> committed",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   stubTransactionStateChecker{},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "(host-a: not started -> committed, host-b: not started -> committed) -> committed",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   stubTransactionStateChecker{},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
						"host-b": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "(host-a: not started -> committed, host-b: prepared -> committed) -> committed",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-b": TransactionPrepared,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
						"host-b": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "(host-a: not started -> rolled back, host-b: prepare failed -> rolled back) -> rolled back",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-b": TransactionPrepareFailed,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
						"host-b": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p1"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeRolledBack,
		},
		{
			name: "(host-a: not started -> rolled back, host-b: prepared -> rolled back, host-c: prepare failed -> rolled back) -> rolled back",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker: stubTransactionStateChecker{
						stateByParticipantID: map[string]TransactionState{
							"host-b": TransactionPrepared,
							"host-c": TransactionPrepareFailed,
						},
					},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
						"host-b": &stubClient{},
						"host-c": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
						{ParticipantID: "host-c", Payload: "p3"},
					},
				},
			},
			wantErrs:    nil,
			wantOutcome: OutcomeRolledBack,
		},
		// TODO: both phase spanning retires - if persistence failed

		// TODO: both phase spanning retires failures - if persistence failed

		{
			name: "committed despite not working persistence",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: stubStatePersister[string]{err: errPersist},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
						"host-b": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxWithTimeout(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
						{ParticipantID: "host-b", Payload: "p2"},
					},
				},
			},
			wantErrs:    []error{errPersist},
			wantOutcome: OutcomeCommitted,
		},
		{
			name: "host client not registered, new client fails init, outcome inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					NewClientFunc: newStubClientFunc(nil, errNewClient),
				},
			},
			args: args{
				ctxFunc: ctxWithTimeout(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    []error{errNewClient},
			wantOutcome: OutcomeInconsistent,
		},
		{
			name: "init state loading fails, outcome inconsistent",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   stubTransactionStateChecker{err: errStateLoad},
					TransactionStatePersister: stubStatePersister[string]{},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": &stubClient{},
					},
				},
			},
			args: args{
				ctxFunc: ctxBackground(),
				distributedTransaction: DistributedTransaction[string]{
					TransactionID: "tx",
					Transactions: []Transaction[string]{
						{ParticipantID: "host-a", Payload: "p1"},
					},
				},
			},
			wantErrs:    []error{errStateLoad},
			wantOutcome: OutcomeInconsistent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			coordinator := NewCoordinator(
				tt.fields.persistenceConfig,
				tt.fields.clientConfig,
			)
			result := coordinator.Execute(tt.args.ctxFunc(), tt.args.distributedTransaction)

			fmt.Printf("err: %s", result.Err())
			for _, err := range tt.wantErrs {
				if !errors.Is(result.Err(), err) {
					t.Errorf("Execute() error = %v, wantErr %v", result.Err(), tt.wantErrs)
				}
			}

			if result.Outcome() != tt.wantOutcome {
				t.Errorf("Execute() outcome = %v, wantOutcome %v", result.Outcome(), tt.wantOutcome)
			}
		})
	}
}

func ctxBackground() func() context.Context {
	return func() context.Context {
		return context.Background()
	}
}

func ctxWithTimeout() func() context.Context {
	maxTestTime := 10 * time.Millisecond
	return func() context.Context {
		newCtx, cancel := context.WithTimeout(context.Background(), maxTestTime)
		_ = cancel
		return newCtx
	}
}

type stubStatePersister[ID comparable] struct {
	err error
}

func (sp stubStatePersister[ID]) PersistState(context.Context, string, ID, TransactionState) error {
	return sp.err
}

type stubClient struct {
	prepareErr  error
	commitErr   error
	rollbackErr error
}

func (c *stubClient) PrepareTransaction(_ context.Context, _ string, _ PreparePayload) error {
	return c.prepareErr
}

func (c *stubClient) CommitTransaction(_ context.Context, _ string) error {
	return c.commitErr
}

func (c *stubClient) RollbackTransaction(_ context.Context, _ string) error {
	return c.rollbackErr
}

func newStubClientFunc(hostToClient map[string]Client, err error) func(participantID string) (Client, error) {
	return func(participantID string) (Client, error) {
		if c, ok := hostToClient[participantID]; ok {
			return c, nil
		}
		return nil, err
	}
}

type stubTransactionStateChecker struct {
	stateByParticipantID map[string]TransactionState
	err                  error
}

func (tsc stubTransactionStateChecker) Check(_ context.Context, _ string) (map[string]TransactionState, error) {
	return tsc.stateByParticipantID, tsc.err
}

func allNotStartedChecker() stubTransactionStateChecker {
	return stubTransactionStateChecker{
		stateByParticipantID: map[string]TransactionState{},
	}
}
