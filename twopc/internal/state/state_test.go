package state

import (
	"reflect"
	"testing"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

func Test_state_nextState(t *testing.T) {
	type fields struct {
		prepared      map[string]struct{}
		prepareFailed map[string]struct{}
		committed     map[string]struct{}
		rolledBack    map[string]struct{}
	}
	type args struct {
		successfulTransitions []Transition[string]
		failedTransitions     []Transition[string]
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
		want   State[string]
	}{
		{
			name:   "no transitions returns same state",
			fields: baseState(),
			args: args{
				successfulTransitions: nil,
				failedTransitions:     nil,
			},
			want: State[string]{
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
			},
		},
		{
			name:   "successful prepare transitions notStarted→prepared",
			fields: baseState(),
			args: args{
				successfulTransitions: []Transition[string]{
					prepareTransition("host-a"),
					prepareTransition("host-b"),
				},
				failedTransitions: nil,
			},
			want: State[string]{
				stateSets: stateSets[string]{
					prepared:      hosts("host-a", "host-b"),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
			},
		},
		{
			name:   "failed prepare transitions notStarted→prepareFailed",
			fields: baseState(),
			args: args{
				successfulTransitions: nil,
				failedTransitions: []Transition[string]{
					prepareTransition("host-a"),
				},
			},
			want: State[string]{
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: hosts("host-a"),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
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
				successfulTransitions: []Transition[string]{
					commitTransition("host-a"),
					commitTransition("host-b"),
				},
				failedTransitions: nil,
			},
			want: State[string]{
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: emptyHosts(),
					committed:     hosts("host-a", "host-b"),
					rolledBack:    emptyHosts(),
				},
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
				successfulTransitions: []Transition[string]{
					commitTransition("host-a"),
				},
				failedTransitions: []Transition[string]{
					commitTransition("host-b"),
				},
			},
			want: State[string]{
				stateSets: stateSets[string]{
					prepared:      hosts("host-b"),
					prepareFailed: emptyHosts(),
					committed:     hosts("host-a"),
					rolledBack:    emptyHosts(),
				},
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
				successfulTransitions: []Transition[string]{
					rollbackTransition("host-a", transaction.PrepareFailed),
				},
				failedTransitions: nil,
			},
			want: State[string]{
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    hosts("host-a"),
				},
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
				failedTransitions: []Transition[string]{
					rollbackTransition("host-a", transaction.PrepareFailed),
				},
			},
			want: State[string]{
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: hosts("host-a"),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
			},
		},
		{
			name:   "partial prepare — one succeeds one fails",
			fields: baseState(),
			args: args{
				successfulTransitions: []Transition[string]{
					prepareTransition("host-a"),
				},
				failedTransitions: []Transition[string]{
					prepareTransition("host-b"),
				},
			},
			want: State[string]{
				stateSets: stateSets[string]{
					prepared:      hosts("host-a"),
					prepareFailed: hosts("host-b"),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := State[string]{
				stateSets: stateSets[string]{
					prepared:      tt.fields.prepared,
					prepareFailed: tt.fields.prepareFailed,
					committed:     tt.fields.committed,
					rolledBack:    tt.fields.rolledBack,
				},
			}
			if got := s.NextState(tt.args.successfulTransitions, tt.args.failedTransitions); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NextState() = %v, want %v", got, tt.want)
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
		transitions []Transition[string]
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []Transition[string]
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
			args: args{transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			}},
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
			args: args{transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			}},
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
			args: args{transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			}},
			want: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
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
			args: args{transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			}},
			want: []Transition[string]{
				prepareTransition("host-b"),
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
			args: args{transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			}},
			want: []Transition[string]{
				commitTransition("host-a"),
				commitTransition("host-b"),
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
			args: args{transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			}},
			want: []Transition[string]{
				commitTransition("host-b"),
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
			args: args{transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			}},
			want: []Transition[string]{
				rollbackTransition("host-a", transaction.Prepared),
				rollbackTransition("host-b", transaction.PrepareFailed),
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
			args: args{transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			}},
			want: []Transition[string]{
				rollbackTransition("host-b", transaction.PrepareFailed),
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
			args: args{transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			}},
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
			args: args{transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			}},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := State[string]{
				stateSets: stateSets[string]{
					prepared:      tt.fields.prepared,
					prepareFailed: tt.fields.prepareFailed,
					committed:     tt.fields.committed,
					rolledBack:    tt.fields.rolledBack,
				},
			}
			got, err := s.tryNextTransitions(tt.args.transitions)
			if (err != nil) != tt.wantErr {
				t.Errorf("tryNextStateTransitions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("tryNextStateTransitions() got = %v, want %v", got, tt.want)
			}
		})
	}
}

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
		transitions     []Transition[string]
		successfulHosts []string
		wantFinalState  State[string]
	}{
		{
			name: "all not started: prepare succeeds for all → all prepared",
			initial: fields{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			},
			transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			},
			successfulHosts: []string{"host-a", "host-b"},
			wantFinalState: State[string]{
				stateSets: stateSets[string]{
					prepared:      hosts("host-a", "host-b"),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
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
			transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			},
			successfulHosts: nil,
			wantFinalState: State[string]{
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: hosts("host-a", "host-b"),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
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
			transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			},
			successfulHosts: []string{"host-a"},
			wantFinalState: State[string]{
				stateSets: stateSets[string]{
					prepared:      hosts("host-a"),
					prepareFailed: hosts("host-b"),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
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
			transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			},
			successfulHosts: []string{"host-a", "host-b"},
			wantFinalState: State[string]{
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: emptyHosts(),
					committed:     hosts("host-a", "host-b"),
					rolledBack:    emptyHosts(),
				},
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
			transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			},
			successfulHosts: nil,
			wantFinalState: State[string]{
				stateSets: stateSets[string]{
					prepared:      hosts("host-a", "host-b"),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
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
			transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			},
			successfulHosts: []string{"host-a", "host-b"},
			wantFinalState: State[string]{
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    hosts("host-a", "host-b"),
				},
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
			transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			},
			successfulHosts: nil,
			wantFinalState: State[string]{
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: hosts("host-a", "host-b"),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
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
			transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			},
			successfulHosts: []string{"host-b"},
			wantFinalState: State[string]{
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: emptyHosts(),
					committed:     hosts("host-a", "host-b"),
					rolledBack:    emptyHosts(),
				},
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
			transitions: []Transition[string]{
				prepareTransition("host-a"),
				prepareTransition("host-b"),
			},
			successfulHosts: []string{"host-b"},
			wantFinalState: State[string]{
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    hosts("host-a", "host-b"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := State[string]{
				stateSets: stateSets[string]{
					prepared:      tt.initial.prepared,
					prepareFailed: tt.initial.prepareFailed,
					committed:     tt.initial.committed,
					rolledBack:    tt.initial.rolledBack,
				},
			}

			transitions, err := s.tryNextTransitions(tt.transitions)
			if err != nil {
				t.Fatalf("tryNextStateTransitions() unexpected error: %v", err)
			}

			successSet := make(map[string]struct{}, len(tt.successfulHosts))
			for _, h := range tt.successfulHosts {
				successSet[h] = struct{}{}
			}

			var successful, failed []Transition[string]
			for _, tr := range transitions {
				if _, ok := successSet[tr.clientID]; ok {
					successful = append(successful, tr)
				} else {
					failed = append(failed, tr)
				}
			}

			got := s.NextState(successful, failed)

			if !reflect.DeepEqual(got, tt.wantFinalState) {
				t.Errorf("final state mismatch\ngot  %+v\nwant %+v", got, tt.wantFinalState)
			}
		})
	}
}

func hosts(ids ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		m[id] = struct{}{}
	}
	return m
}

func emptyHosts() map[string]struct{} {
	return map[string]struct{}{}
}
