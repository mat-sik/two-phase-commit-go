package coordinator

import (
	"reflect"
	"testing"
)

func Test_stateLoader_loadState(t *testing.T) {
	type fields struct {
		transactionStateChecker TransactionStateChecker
	}
	type args struct {
		transactionID string
		transactions  []Transaction
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   state
	}{
		{
			name: "all prepare failed",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByHost: map[string]TransactionState{
						"host-a": transactionPrepareFailed,
						"host-b": transactionPrepareFailed,
					},
				},
			},
			args: args{
				transactionID: "tx-1",
				transactions:  []Transaction{txn("host-a"), txn("host-b")},
			},
			want: state{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a", "host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "all prepared",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByHost: map[string]TransactionState{
						"host-a": transactionPrepared,
						"host-b": transactionPrepared,
					},
				},
			},
			args: args{
				transactionID: "tx-2",
				transactions:  []Transaction{txn("host-a"), txn("host-b")},
			},
			want: state{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "mixed states across hosts",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByHost: map[string]TransactionState{
						"host-a": transactionPrepared,
						"host-b": transactionPrepared,
						"host-c": transactionCommitted,
					},
				},
			},
			args: args{
				transactionID: "tx-3",
				transactions:  []Transaction{txn("host-a"), txn("host-b"), txn("host-c")},
			},
			want: state{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-c"),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "all rolled back",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByHost: map[string]TransactionState{
						"host-a": transactionRolledBack,
						"host-b": transactionRolledBack,
					},
				},
			},
			args: args{
				transactionID: "tx-4",
				transactions:  []Transaction{txn("host-a"), txn("host-b")},
			},
			want: state{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a", "host-b"),
			},
		},
		{
			name: "empty transactions list",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByHost: map[string]TransactionState{},
				},
			},
			args: args{
				transactionID: "tx-5",
				transactions:  []Transaction{},
			},
			want: state{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "checker returns state for extra hosts not in transactions — only transactions are mapped",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByHost: map[string]TransactionState{
						"host-a": transactionPrepared,
						"host-z": transactionCommitted, // not in transaction list
					},
				},
			},
			args: args{
				transactionID: "tx-6",
				transactions:  []Transaction{txn("host-a")},
			},
			want: state{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sl := StateLoader{
				transactionStateChecker: tt.fields.transactionStateChecker,
			}
			if got := sl.loadState(tt.args.transactionID, tt.args.transactions); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("loadState() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_state_nextState(t *testing.T) {
	type fields struct {
		prepared      map[string]struct{}
		prepareFailed map[string]struct{}
		committed     map[string]struct{}
		rolledBack    map[string]struct{}
	}
	type args struct {
		successfulTransitions []stateTransition
		failedTransitions     []stateTransition
	}
	baseState := func() fields {
		return fields{
			prepared:      emptyHosts(),
			prepareFailed: emptyHosts(),
			committed:     emptyHosts(),
			rolledBack:    emptyHosts(),
		}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   state
	}{
		{
			name:   "no transitions returns same state",
			fields: baseState(),
			args: args{
				successfulTransitions: nil,
				failedTransitions:     nil,
			},
			want: state{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name:   "successful prepare transitions notStarted→prepared",
			fields: baseState(),
			args: args{
				successfulTransitions: []stateTransition{
					prepareStateTransition{preTransitionState: transactionNotStarted, transaction: txn("host-a")},
					prepareStateTransition{preTransitionState: transactionNotStarted, transaction: txn("host-b")},
				},
				failedTransitions: nil,
			},
			want: state{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name:   "failed prepare transitions notStarted→prepareFailed",
			fields: baseState(),
			args: args{
				successfulTransitions: nil,
				failedTransitions: []stateTransition{
					prepareStateTransition{preTransitionState: transactionNotStarted, transaction: txn("host-a")},
				},
			},
			want: state{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "successful commit transitions prepared→committed",
			fields: fields{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			args: args{
				successfulTransitions: []stateTransition{
					commitStateTransition{preTransitionState: transactionPrepared, transaction: txn("host-a")},
					commitStateTransition{preTransitionState: transactionPrepared, transaction: txn("host-b")},
				},
				failedTransitions: nil,
			},
			want: state{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a", "host-b"),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "failed commit keeps host in prepared state",
			fields: fields{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			args: args{
				successfulTransitions: []stateTransition{
					commitStateTransition{preTransitionState: transactionPrepared, transaction: txn("host-a")},
				},
				failedTransitions: []stateTransition{
					commitStateTransition{preTransitionState: transactionPrepared, transaction: txn("host-b")},
				},
			},
			want: state{
				prepared:      hosts("host-b"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "successful rollback transitions prepareFailed→rolledBack",
			fields: fields{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			args: args{
				successfulTransitions: []stateTransition{
					rollbackStateTransition{preTransitionState: transactionPrepareFailed, transaction: txn("host-a")},
				},
				failedTransitions: nil,
			},
			want: state{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a"),
			},
		},
		{
			name: "failed rollback keeps host in prepareFailed state",
			fields: fields{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			args: args{
				successfulTransitions: nil,
				failedTransitions: []stateTransition{
					rollbackStateTransition{preTransitionState: transactionPrepareFailed, transaction: txn("host-a")},
				},
			},
			want: state{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "partial prepare — one succeeds one fails",
			fields: fields{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			args: args{
				successfulTransitions: []stateTransition{
					prepareStateTransition{preTransitionState: transactionNotStarted, transaction: txn("host-a")},
				},
				failedTransitions: []stateTransition{
					prepareStateTransition{preTransitionState: transactionNotStarted, transaction: txn("host-b")},
				},
			},
			want: state{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := state{
				prepared:      tt.fields.prepared,
				prepareFailed: tt.fields.prepareFailed,
				committed:     tt.fields.committed,
				rolledBack:    tt.fields.rolledBack,
			}
			if got := s.nextState(tt.args.successfulTransitions, tt.args.failedTransitions); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("nextState() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_state_nextStateTransitions(t *testing.T) {
	type fields struct {
		prepared      map[string]struct{}
		prepareFailed map[string]struct{}
		committed     map[string]struct{}
		rolledBack    map[string]struct{}
	}
	type args struct {
		transactions []Transaction
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []stateTransition
		wantErr bool
	}{
		// ── Terminal states: no more transitions ──────────────────────────────
		{
			name: "all committed — no transitions needed",
			fields: fields{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a", "host-b"),
				rolledBack:    emptyHosts(),
			},
			args:    args{transactions: []Transaction{txn("host-a"), txn("host-b")}},
			want:    nil,
			wantErr: false,
		},
		{
			name: "all rolled back — no transitions needed",
			fields: fields{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a", "host-b"),
			},
			args:    args{transactions: []Transaction{txn("host-a"), txn("host-b")}},
			want:    nil,
			wantErr: false,
		},

		// ── Happy path: prepare phase ─────────────────────────────────────────
		{
			name: "all not started — issue prepare for all",
			fields: fields{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			args: args{transactions: []Transaction{txn("host-a"), txn("host-b")}},
			want: []stateTransition{
				prepareStateTransition{preTransitionState: transactionNotStarted, transaction: txn("host-a")},
				prepareStateTransition{preTransitionState: transactionNotStarted, transaction: txn("host-b")},
			},
			wantErr: false,
		},
		{
			name: "one prepared one not started — only prepare the not-started host",
			fields: fields{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			args: args{transactions: []Transaction{txn("host-a"), txn("host-b")}},
			want: []stateTransition{
				prepareStateTransition{preTransitionState: transactionNotStarted, transaction: txn("host-b")},
			},
			wantErr: false,
		},

		// ── Happy path: commit phase ──────────────────────────────────────────
		{
			name: "all prepared — issue commit for all",
			fields: fields{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			args: args{transactions: []Transaction{txn("host-a"), txn("host-b")}},
			want: []stateTransition{
				commitStateTransition{preTransitionState: transactionPrepared, transaction: txn("host-a")},
				commitStateTransition{preTransitionState: transactionPrepared, transaction: txn("host-b")},
			},
			wantErr: false,
		},
		{
			name: "one committed one prepared — only commit the remaining prepared host",
			fields: fields{
				prepared:      hosts("host-b"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    emptyHosts(),
			},
			args: args{transactions: []Transaction{txn("host-a"), txn("host-b")}},
			want: []stateTransition{
				commitStateTransition{preTransitionState: transactionPrepared, transaction: txn("host-b")},
			},
			wantErr: false,
		},

		// ── Rollback path ─────────────────────────────────────────────────────
		{
			name: "prepare failed — issue rollback for all non-rolled-back hosts",
			fields: fields{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			args: args{transactions: []Transaction{txn("host-a"), txn("host-b")}},
			want: []stateTransition{
				rollbackStateTransition{preTransitionState: transactionPrepared, transaction: txn("host-a")},
				rollbackStateTransition{preTransitionState: transactionPrepareFailed, transaction: txn("host-b")},
			},
			wantErr: false,
		},
		{
			name: "prepare failed — skip already rolled-back hosts",
			fields: fields{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a"),
			},
			args: args{transactions: []Transaction{txn("host-a"), txn("host-b")}},
			want: []stateTransition{
				rollbackStateTransition{preTransitionState: transactionPrepareFailed, transaction: txn("host-b")},
			},
			wantErr: false,
		},

		// ── Invalid states ────────────────────────────────────────────────────
		{
			name: "committed and rolled back simultaneously — invalid state error",
			fields: fields{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    hosts("host-b"),
			},
			args:    args{transactions: []Transaction{txn("host-a"), txn("host-b")}},
			want:    nil,
			wantErr: true,
		},
		{
			name: "prepareFailed and committed simultaneously — invalid state error",
			fields: fields{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a"),
				committed:     hosts("host-b"),
				rolledBack:    emptyHosts(),
			},
			args:    args{transactions: []Transaction{txn("host-a"), txn("host-b")}},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := state{
				prepared:      tt.fields.prepared,
				prepareFailed: tt.fields.prepareFailed,
				committed:     tt.fields.committed,
				rolledBack:    tt.fields.rolledBack,
			}
			got, err := s.tryNextStateTransitions(tt.args.transactions)
			if (err != nil) != tt.wantErr {
				t.Errorf("nextStateTransitions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("nextStateTransitions() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// ─── round-trip: initial state → tryNextStateTransitions → nextState ─────────

func Test_state_roundTrip(t *testing.T) {
	type fields struct {
		prepared      map[string]struct{}
		prepareFailed map[string]struct{}
		committed     map[string]struct{}
		rolledBack    map[string]struct{}
	}
	tests := []struct {
		name            string
		initial         fields
		transactions    []Transaction
		successfulHosts []string // hosts whose generated transitions succeed this round
		failedHosts     []string // hosts whose generated transitions fail this round
		wantFinalState  state
	}{
		{
			name: "all not started: prepare succeeds for all → all prepared",
			initial: fields{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			transactions:    []Transaction{txn("host-a"), txn("host-b")},
			successfulHosts: []string{"host-a", "host-b"},
			failedHosts:     nil,
			wantFinalState: state{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "all not started: prepare fails for all → all prepareFailed",
			initial: fields{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			transactions:    []Transaction{txn("host-a"), txn("host-b")},
			successfulHosts: nil,
			failedHosts:     []string{"host-a", "host-b"},
			wantFinalState: state{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a", "host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "all not started: prepare succeeds for one fails for other → one prepared one prepareFailed",
			initial: fields{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			transactions:    []Transaction{txn("host-a"), txn("host-b")},
			successfulHosts: []string{"host-a"},
			failedHosts:     []string{"host-b"},
			wantFinalState: state{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "all prepared: commit succeeds for all → all committed",
			initial: fields{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			transactions:    []Transaction{txn("host-a"), txn("host-b")},
			successfulHosts: []string{"host-a", "host-b"},
			failedHosts:     nil,
			wantFinalState: state{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a", "host-b"),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "all prepared: commit fails for all → remain prepared",
			initial: fields{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			transactions:    []Transaction{txn("host-a"), txn("host-b")},
			successfulHosts: nil,
			failedHosts:     []string{"host-a", "host-b"},
			wantFinalState: state{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "prepareFailed present: rollback succeeds for all → all rolledBack",
			initial: fields{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			transactions:    []Transaction{txn("host-a"), txn("host-b")},
			successfulHosts: []string{"host-a", "host-b"},
			failedHosts:     nil,
			wantFinalState: state{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a", "host-b"),
			},
		},
		{
			name: "prepareFailed present: rollback fails for all → prepared becomes prepareFailed, prepareFailed stays",
			initial: fields{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			transactions:    []Transaction{txn("host-a"), txn("host-b")},
			successfulHosts: nil,
			failedHosts:     []string{"host-a", "host-b"},
			// failed rollback: prepared→prepareFailed, prepareFailed→prepareFailed
			wantFinalState: state{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a", "host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "partial commit already done: commit remaining host succeeds → all committed",
			initial: fields{
				prepared:      hosts("host-b"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    emptyHosts(),
			},
			transactions:    []Transaction{txn("host-a"), txn("host-b")},
			successfulHosts: []string{"host-b"},
			failedHosts:     nil,
			wantFinalState: state{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a", "host-b"),
				rolledBack:    emptyHosts(),
			},
		},
		{
			name: "partial rollback already done: rollback remaining host succeeds → all rolledBack",
			initial: fields{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a"),
			},
			transactions:    []Transaction{txn("host-a"), txn("host-b")},
			successfulHosts: []string{"host-b"},
			failedHosts:     nil,
			wantFinalState: state{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a", "host-b"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := state{
				prepared:      tt.initial.prepared,
				prepareFailed: tt.initial.prepareFailed,
				committed:     tt.initial.committed,
				rolledBack:    tt.initial.rolledBack,
			}

			transitions, err := s.tryNextStateTransitions(tt.transactions)
			if err != nil {
				t.Fatalf("tryNextStateTransitions() unexpected error: %v", err)
			}

			// split generated transitions into successful/failed based on the
			// host sets declared in the test case
			successSet := make(map[string]struct{}, len(tt.successfulHosts))
			for _, h := range tt.successfulHosts {
				successSet[h] = struct{}{}
			}

			var successful, failed []stateTransition
			for _, tr := range transitions {
				if _, ok := successSet[tr.host()]; ok {
					successful = append(successful, tr)
				} else {
					failed = append(failed, tr)
				}
			}

			got := s.nextState(successful, failed)

			if !reflect.DeepEqual(got, tt.wantFinalState) {
				t.Errorf("final state mismatch\ngot  %+v\nwant %+v", got, tt.wantFinalState)
			}
		})
	}
}

type mockTransactionStateChecker struct {
	stateByHost map[string]TransactionState
}

func (m mockTransactionStateChecker) Check(_ string) map[string]TransactionState {
	return m.stateByHost
}

func hosts(hs ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(hs))
	for _, h := range hs {
		m[h] = struct{}{}
	}
	return m
}

func emptyHosts() map[string]struct{} {
	return map[string]struct{}{}
}

func txn(host string) Transaction {
	return Transaction{TargetHost: host}
}
