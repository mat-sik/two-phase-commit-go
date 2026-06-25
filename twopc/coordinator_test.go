package twopc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestCoordinator_Execute(t *testing.T) {
	tests := []compactedTestCase{
		// ═════════════════════════════════════════════════════════════════════════════
		// GROUP 0 - no coordinator failures
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name:        "NS -> P -> C",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
			},
		},
		{
			name: "NS -> PF",
			// TODO: Maybe this outcome should be renamed so that we can distinguish these two terminal states
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepare(),
				},
			},
		},
		{
			name:        "a: NS -> P -> C, b: NS -> P -> C",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionNotStarted,
				},
			},
		},
		{
			name:        "a: NS -> PF, b: NS -> PF",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepare(),
				},
				{
					id:     "host-b",
					state:  TransactionNotStarted,
					client: failPrepare(),
				},
			},
		},
		{
			name:        "a: NS -> P -> R, b: NS -> PF",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:     "host-b",
					state:  TransactionNotStarted,
					client: failPrepare(),
				},
			},
		},
		// ═════════════════════════════════════════════════════════════════════════════
		// GROUP 1 - retry from coordinator failure
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name:        "a: P(NS) -> C, b: P -> C",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepared,
				},
			},
		},
		{
			name:        "(NS,PF)",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepareFailed,
				},
			},
		},
		{
			name:        "a: P(NS) -> C, b: C",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionCommitted,
				},
			},
		},
		{
			name:        "a: P(NS) -> R, b: R",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionRolledBack,
				},
			},
		},
		{
			name:        "a: P(NS) -> C, b: P(NS) -> C, c: P(NS) -> C",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionNotStarted,
				},
				{
					id:    "host-c",
					state: TransactionNotStarted,
				},
			},
		},
		{
			name:        "a: P(NS) -> C, b: P(NS) -> C, c: P -> C",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionNotStarted,
				},
				{
					id:    "host-c",
					state: TransactionPrepared,
				},
			},
		},
		{
			name:        "(NS,NS,PF)",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionNotStarted,
				},
				{
					id:    "host-c",
					state: TransactionPrepareFailed,
				},
			},
		},
		{
			name:        "a: P(NS) -> C, b: P(NS) -> C, c: C",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionNotStarted,
				},
				{
					id:    "host-c",
					state: TransactionCommitted,
				},
			},
		},
		{
			name:        "(NS,NS,R)",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionNotStarted,
				},
				{
					id:    "host-c",
					state: TransactionRolledBack,
				},
			},
		},
		{
			name:        "a: P(NS) -> C, host-b: P -> C, host-c: P -> C",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepared,
				},
				{
					id:    "host-c",
					state: TransactionPrepared,
				},
			},
		},
		{
			name:        "a: NS, b: P -> R, c: PF -> R",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepared,
				},
				{
					id:    "host-c",
					state: TransactionPrepareFailed,
				},
			},
		},
		{
			name:        "a: P(NS) -> C, b: P -> C, c: C",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepared,
				},
				{
					id:    "host-c",
					state: TransactionCommitted,
				},
			},
		},
		{
			name:        "a: NS, b: P -> R, c: R",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepared,
				},
				{
					id:    "host-c",
					state: TransactionRolledBack,
				},
			},
		},
		{
			name:        "(NS,PF,PF)",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepareFailed,
				},
				{
					id:    "host-c",
					state: TransactionPrepareFailed,
				},
			},
		},
		{
			name:        "(NS,PF,R)",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepareFailed,
				},
				{
					id:    "host-c",
					state: TransactionRolledBack,
				},
			},
		},
		{
			name:        "(NS,C,C)",
			wantOutcome: OutcomeCommitted,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionCommitted,
				},
				{
					id:    "host-c",
					state: TransactionCommitted,
				},
			},
		},
		{
			name:        "(NS,R,R)",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionRolledBack,
				},
				{
					id:    "host-c",
					state: TransactionRolledBack,
				},
			},
		},
		{
			name:        "a: P(NS) -> R, b: P -> R, c: PF, d: R",
			wantOutcome: OutcomeRolledBack,
			participants: []participantConfig{
				{
					id:    "host-a",
					state: TransactionNotStarted,
				},
				{
					id:    "host-b",
					state: TransactionPrepared,
				},
				{
					id:    "host-c",
					state: TransactionPrepareFailed,
				},
				{
					id:    "host-d",
					state: TransactionRolledBack,
				},
			},
		},
		// ═════════════════════════════════════════════════════════════════════════════
		// GROUP 2 - coordinator failures
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name:        "NS -> P -x-> C",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failCommit(),
				},
			},
		},
		{
			name:        "P -x-> C",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepared,
					client: failCommit(),
				},
			},
		},
		{
			name:        "a: NS -> P -x-> C, b: NS -> P -x-> C",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failCommit(),
				},
				{
					id:     "host-b",
					state:  TransactionNotStarted,
					client: failCommit(),
				},
			},
		},
		{
			name:        "a: NS -> P -x-> C, b: NS -> P -> C",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failCommit(),
				},
				{
					id:    "host-b",
					state: TransactionNotStarted,
				},
			},
		},
		{
			name:        "a: NS -> P -x-> R, b: NS -> PF",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failRollback(),
				},
				{
					id:     "host-b",
					state:  TransactionNotStarted,
					client: failPrepare(),
				},
			},
		},
		{
			name:        "a: NS -> P -x-> C, b: P -x-> C",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failCommit(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepared,
					client: failCommit(),
				},
			},
		},
		{
			name:        "a: NS -> P -x-> C, b: P -> C",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failCommit(),
				},
				{
					id:    "host-b",
					state: TransactionPrepared,
				},
			},
		},
		{
			name:        "a: NS -> PF, b: P -x-> R",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failPrepare(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepared,
					client: failRollback(),
				},
			},
		},
		{
			name:        "a: NS -> P -x-> R, b: PF",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failRollback(),
				},
				{
					id:    "host-b",
					state: TransactionPrepareFailed,
				},
			},
		},
		{
			name:        "a: P(NS) -x-> C, b: C",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failCommit(),
				},
				{
					id:    "host-b",
					state: TransactionCommitted,
				},
			},
		},
		{
			name:        "a: P(NS) -x-> R, b: R",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionNotStarted,
					client: failRollback(),
				},
				{
					id:    "host-b",
					state: TransactionRolledBack,
				},
			},
		},
		{
			name:        "a: P -x-> C, b: P -x-> C",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepared,
					client: failCommit(),
				},
				{
					id:     "host-b",
					state:  TransactionPrepared,
					client: failCommit(),
				},
			},
		},
		{
			name:        "a: P -x-> C, b: P -> C",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepared,
					client: failCommit(),
				},
				{
					id:    "host-b",
					state: TransactionPrepared,
				},
			},
		},
		{
			name:        "a: P -x-> R, b: PF",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepared,
					client: failRollback(),
				},
				{
					id:    "host-b",
					state: TransactionPrepareFailed,
				},
			},
		},
		{
			name:        "a: P -x-> C, b: C",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepared,
					client: failCommit(),
				},
				{
					id:    "host-b",
					state: TransactionCommitted,
				},
			},
		},
		{
			name:        "a: P -x-> R, b: R",
			wantOutcome: OutcomeInconsistent,
			participants: []participantConfig{
				{
					id:     "host-a",
					state:  TransactionPrepared,
					client: failRollback(),
				},
				{
					id:    "host-b",
					state: TransactionRolledBack,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run(t, newCase(tt))
		})
	}
}

func TestCoordinator_Execute_dependencies_fails(t *testing.T) {
	tests := []testCase{
		{
			name: "committed despite not working persistence and used mixed client construction",
			fields: fields{
				persistenceConfig: PersistenceConfig[string]{
					TransactionStateChecker:   allNotStartedChecker(),
					TransactionStatePersister: stubStatePersister[string]{err: errPersist},
				},
				clientConfig: ClientConfig[string]{
					Clients: map[string]Client{
						"host-a": ok(),
					},
					NewClientFunc: newStubClientFunc(
						map[string]Client{
							"host-b": ok(),
						}, nil,
					),
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
			wantOutcome: OutcomeRolledBack,
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
						"host-a": ok(),
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
			run(t, tt)
		})
	}
}

func run(t *testing.T, tt testCase) {
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
}

type participantConfig struct {
	id     string
	state  TransactionState
	client *stubClient
}

type testCase struct {
	name        string
	fields      fields
	args        args
	wantErrs    []error
	wantOutcome Outcome
}

type fields struct {
	persistenceConfig PersistenceConfig[string]
	clientConfig      ClientConfig[string]
}
type args struct {
	ctxFunc                func() context.Context
	distributedTransaction DistributedTransaction[string]
}

type compactedTestCase struct {
	name         string
	ctxFunc      func() context.Context
	wantOutcome  Outcome
	wantErrs     []error
	participants []participantConfig
}

func newCase(data compactedTestCase) testCase {
	checker, clients, wantErrs, txs := scenario(data.participants...)

	var tc testCase
	tc.name = data.name
	tc.fields.persistenceConfig = PersistenceConfig[string]{
		TransactionStateChecker:   checker,
		TransactionStatePersister: stubStatePersister[string]{},
	}
	tc.fields.clientConfig = ClientConfig[string]{Clients: clients}
	if data.ctxFunc == nil {
		data.ctxFunc = ctxBackground()
	}
	if data.wantOutcome == OutcomeInconsistent {
		data.ctxFunc = ctxWithTimeout()
	}
	tc.args.ctxFunc = data.ctxFunc
	tc.args.distributedTransaction = DistributedTransaction[string]{
		TransactionID: "tx",
		Transactions:  txs,
	}
	tc.wantErrs = append(data.wantErrs, wantErrs...)
	tc.wantOutcome = data.wantOutcome
	return tc
}

func scenario(participants ...participantConfig) (stubTransactionStateChecker, map[string]Client, []error, []Transaction[string]) {
	states := map[string]TransactionState{}
	clients := map[string]Client{}
	var expectedClientsErrs []error
	txs := make([]Transaction[string], 0, len(participants))

	for i, p := range participants {
		if p.state != TransactionNotStarted {
			states[p.id] = p.state
		}
		if p.client == nil {
			p.client = ok()
		}

		if p.client.prepareErr != nil {
			expectedClientsErrs = append(expectedClientsErrs, errPrepare)
		}
		if p.client.commitErr != nil {
			expectedClientsErrs = append(expectedClientsErrs, errCommit)
		} else if p.client.rollbackErr != nil {
			expectedClientsErrs = append(expectedClientsErrs, errRollback)
		}

		clients[p.id] = p.client
		txs = append(txs, Transaction[string]{
			ParticipantID: p.id,
			Payload:       fmt.Sprintf("p%d", i+1),
		})
	}
	return stubTransactionStateChecker{stateByParticipantID: states}, clients, expectedClientsErrs, txs
}

var (
	errPrepare   = errors.New("prepare failed")
	errCommit    = errors.New("commit failed")
	errRollback  = errors.New("rollback failed")
	errPersist   = errors.New("persist failed")
	errNewClient = errors.New("create new client failed")
	errStateLoad = errors.New("state load failed")
)

func ok() *stubClient {
	return &stubClient{}
}

func failPrepare() *stubClient {
	return &stubClient{prepareErr: errPrepare}
}

func failPrepareAndRollback() *stubClient {
	return &stubClient{prepareErr: errPrepare, rollbackErr: errRollback}
}

func failCommit() *stubClient {
	return &stubClient{commitErr: errCommit}
}

func failRollback() *stubClient {
	return &stubClient{rollbackErr: errRollback}
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
