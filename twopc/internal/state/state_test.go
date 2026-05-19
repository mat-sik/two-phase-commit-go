package state

import (
	"reflect"
	"sort"
	"testing"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

func Test_state_nextState(t *testing.T) {
	type args struct {
		successfulTransitions []Transition[string]
		failedTransitions     []Transition[string]
	}
	tests := []struct {
		name  string
		state State[string]
		args  args
		want  State[string]
	}{
		{
			name: "no transitions returns same state",
			state: newState(stateSets[string]{
				prepared:      hosts("host-b"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				successfulTransitions: nil,
				failedTransitions:     nil,
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-b"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "successful prepare 2x notStarted→prepared",
			state: State[string]{
				participantIDs: map[string]struct{}{
					"host-a": {},
					"host-b": {},
				},
				stateSets: baseStateSets(),
			},
			args: args{
				successfulTransitions: []Transition[string]{
					PrepareTransition("host-a"),
					PrepareTransition("host-b"),
				},
				failedTransitions: nil,
			},
			want: State[string]{
				participantIDs: map[string]struct{}{
					"host-a": {},
					"host-b": {},
				},
				stateSets: stateSets[string]{
					prepared:      hosts("host-a", "host-b"),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
			},
		},
		{
			name: "failed prepare notStarted→prepared,notStarted→prepareFailed",
			state: State[string]{
				participantIDs: map[string]struct{}{
					"host-a": {},
					"host-b": {},
				},
				stateSets: baseStateSets(),
			},
			args: args{
				successfulTransitions: []Transition[string]{
					PrepareTransition("host-a"),
				},
				failedTransitions: []Transition[string]{
					PrepareTransition("host-b"),
				},
			},
			want: State[string]{
				participantIDs: map[string]struct{}{
					"host-a": {},
					"host-b": {},
				},
				stateSets: stateSets[string]{
					prepared:      hosts("host-a"),
					prepareFailed: hosts("host-b"),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
			},
		},
		{
			name: "failed prepare notStarted->prepared,notStarted->prepareFailed",
			state: State[string]{
				participantIDs: map[string]struct{}{
					"host-a": {},
					"host-b": {},
				},
				stateSets: baseStateSets(),
			},
			args: args{
				successfulTransitions: []Transition[string]{
					PrepareTransition("host-a"),
				},
				failedTransitions: []Transition[string]{
					PrepareTransition("host-b"),
				},
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "successful commit 2x prepared→committed",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				successfulTransitions: []Transition[string]{
					CommitTransition("host-a"),
					CommitTransition("host-b"),
				},
				failedTransitions: nil,
			},
			want: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a", "host-b"),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "failed commit prepared->committed,prepared-prepared,commited->commited",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-c"),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				successfulTransitions: []Transition[string]{
					CommitTransition("host-a"),
				},
				failedTransitions: []Transition[string]{
					CommitTransition("host-b"),
				},
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-b"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a", "host-c"),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "successful rollback prepared->rolledBack,prepareFailed→rolledBack",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				successfulTransitions: []Transition[string]{
					RollbackTransition("host-a", transaction.Prepared),
					RollbackTransition("host-b", transaction.PrepareFailed),
				},
				failedTransitions: nil,
			},
			want: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a", "host-b"),
			}),
		},
		{
			name: "failed rollback prepared->prepareFailed,prepareFailed->prepareFailed,rolledBack->rolledBack",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-c"),
			}),
			args: args{
				successfulTransitions: nil,
				failedTransitions: []Transition[string]{
					RollbackTransition("host-a", transaction.Prepared),
					RollbackTransition("host-b", transaction.PrepareFailed),
				},
			},
			want: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a", "host-b"),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-c"),
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.state

			s.NextState(tt.args.successfulTransitions, tt.args.failedTransitions)

			if !reflect.DeepEqual(s, tt.want) {
				t.Errorf("NextState() = %v, want %v", s, tt.want)
			}
		})
	}
}

func Test_state_nextStateTransitions(t *testing.T) {
	tests := []struct {
		name    string
		state   State[string]
		want    []Transition[string]
		wantErr bool
	}{
		// ── Terminal states: no more transitions ──────────────────────────────
		{
			name: "all committed — no transitions needed",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a", "host-b"),
				rolledBack:    emptyHosts(),
			}),
			want:    nil,
			wantErr: false,
		},
		{
			name: "all rolled back — no transitions needed",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a", "host-b"),
			}),
			want:    nil,
			wantErr: false,
		},

		// ── Happy path: prepare phase ─────────────────────────────────────────
		{
			name: "all not started — issue prepare for all",
			state: State[string]{
				participantIDs: map[string]struct{}{
					"host-a": {},
					"host-b": {},
				},
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
			},
			want: []Transition[string]{
				PrepareTransition("host-a"),
				PrepareTransition("host-b"),
			},
			wantErr: false,
		},
		{
			name: "one prepared one not started — only prepare the not-started host",
			state: State[string]{
				participantIDs: map[string]struct{}{
					"host-a": {},
					"host-b": {},
				},
				stateSets: stateSets[string]{
					prepared:      hosts("host-a"),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
			},
			want: []Transition[string]{
				PrepareTransition("host-b"),
			},
			wantErr: false,
		},

		// ── Happy path: commit phase ──────────────────────────────────────────
		{
			name: "all prepared — issue commit for all",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			want: []Transition[string]{
				CommitTransition("host-a"),
				CommitTransition("host-b"),
			},
			wantErr: false,
		},
		{
			name: "one committed one prepared — only commit the remaining prepared host",
			state: newState(stateSets[string]{
				prepared:      hosts("host-b"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    emptyHosts(),
			}),
			want: []Transition[string]{
				CommitTransition("host-b"),
			},
			wantErr: false,
		},

		// ── Rollback path ─────────────────────────────────────────────────────
		{
			name: "prepare failed — issue rollback for all non-rolled-back hosts",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			want: []Transition[string]{
				RollbackTransition("host-a", transaction.Prepared),
				RollbackTransition("host-b", transaction.PrepareFailed),
			},
			wantErr: false,
		},
		{
			name: "prepare failed — skip already rolled-back hosts",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a"),
			}),
			want: []Transition[string]{
				RollbackTransition("host-b", transaction.PrepareFailed),
			},
			wantErr: false,
		},

		// ── Invalid states ────────────────────────────────────────────────────
		{
			name: "committed and rolled back simultaneously — invalid state error",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    hosts("host-b"),
			}),
			want:    nil,
			wantErr: true,
		},
		{
			name: "prepareFailed and committed simultaneously — invalid state error",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a"),
				committed:     hosts("host-b"),
				rolledBack:    emptyHosts(),
			}),
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.state.tryNextTransitions()
			if (err != nil) != tt.wantErr {
				t.Errorf("tryNextStateTransitions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			sortTransitions(got)
			sortTransitions(tt.want)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("tryNextStateTransitions() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func sortTransitions(v []Transition[string]) {
	sort.Slice(v, func(i, j int) bool {
		return v[i].participantID < v[j].participantID
	})
}

func Test_state_roundTrip(t *testing.T) {
	tests := []struct {
		name            string
		state           State[string]
		successfulHosts []string
		wantFinalState  State[string]
	}{
		// ── prepare phase ─────────────────────────────────────────────────────
		{
			name: "all not started: prepare succeeds for all → all prepared",
			state: State[string]{
				participantIDs: map[string]struct{}{
					"host-a": {},
					"host-b": {},
				},
				stateSets: baseStateSets(),
			},
			successfulHosts: []string{"host-a", "host-b"},
			wantFinalState: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "all not started: prepare fails for all → all prepareFailed",
			state: State[string]{
				participantIDs: map[string]struct{}{
					"host-a": {},
					"host-b": {},
				},
				stateSets: baseStateSets(),
			},
			successfulHosts: nil,
			wantFinalState: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a", "host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "all not started: prepare succeeds for one fails for the other → one prepared one prepareFailed",
			state: State[string]{
				participantIDs: map[string]struct{}{
					"host-a": {},
					"host-b": {},
				},
				stateSets: baseStateSets(),
			},
			successfulHosts: []string{"host-a"},
			wantFinalState: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		// ── commit phase ──────────────────────────────────────────────────────
		{
			name: "all prepared: commit succeeds for all → all committed",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			successfulHosts: []string{"host-a", "host-b"},
			wantFinalState: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a", "host-b"),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "all prepared: commit fails for all → remain prepared",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			successfulHosts: nil,
			wantFinalState: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "all prepared: commit fails for one → one remains prepared",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			successfulHosts: []string{"host-a"},
			wantFinalState: newState(stateSets[string]{
				prepared:      hosts("host-b"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "partial commit already done: commit remaining host succeeds → all committed",
			state: newState(stateSets[string]{
				prepared:      hosts("host-b"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    emptyHosts(),
			}),
			successfulHosts: []string{"host-b"},
			wantFinalState: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a", "host-b"),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "partial commit already done: commit remaining host fails → remains prepared",
			state: newState(stateSets[string]{
				prepared:      hosts("host-b"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    emptyHosts(),
			}),
			successfulHosts: nil,
			wantFinalState: newState(stateSets[string]{
				prepared:      hosts("host-b"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    emptyHosts(),
			}),
		},
		// ── rollback phase ────────────────────────────────────────────────────
		{
			name: "prepareFailed present: rollback succeeds for all → all rolledBack",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			successfulHosts: []string{"host-a", "host-b"},
			wantFinalState: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a", "host-b"),
			}),
		},
		{
			name: "prepareFailed present: rollback fails for all → prepared->prepareFailed, prepareFailed->prepareFailed",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			successfulHosts: nil,
			wantFinalState: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a", "host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "prepareFailed present: rollback fails for one → prepared->prepareFailed, prepareFailed->rolledBack",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			successfulHosts: []string{"host-b"},
			wantFinalState: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a"),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-b"),
			}),
		},
		{
			name: "partial rollback already done: rollback remaining host succeeds → all rolledBack",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a"),
			}),
			successfulHosts: []string{"host-b"},
			wantFinalState: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a", "host-b"),
			}),
		},
		{
			name: "partial rollback already done: rollback remaining host fails → stays prepareFailed",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a"),
			}),
			successfulHosts: nil,
			wantFinalState: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a"),
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.state

			transitions, err := s.tryNextTransitions()
			if err != nil {
				t.Fatalf("tryNextStateTransitions() unexpected error: %v", err)
			}

			successSet := make(map[string]struct{}, len(tt.successfulHosts))
			for _, h := range tt.successfulHosts {
				successSet[h] = struct{}{}
			}

			var successful, failed []Transition[string]
			for _, tr := range transitions {
				if _, ok := successSet[tr.participantID]; ok {
					successful = append(successful, tr)
				} else {
					failed = append(failed, tr)
				}
			}

			s.NextState(successful, failed)

			if !reflect.DeepEqual(s, tt.wantFinalState) {
				t.Errorf("final state mismatch\ngot  %+v\nwant %+v", s, tt.wantFinalState)
			}
		})
	}
}

func baseStateSets() stateSets[string] {
	return stateSets[string]{
		prepared:      emptyHosts(),
		prepareFailed: emptyHosts(),
		committed:     emptyHosts(),
		rolledBack:    emptyHosts(),
	}
}

func newState(ss stateSets[string]) State[string] {
	return State[string]{
		participantIDs: participantIDs(ss),
		stateSets: stateSets[string]{
			prepared:      ss.prepared,
			prepareFailed: ss.prepareFailed,
			committed:     ss.committed,
			rolledBack:    ss.rolledBack,
		},
	}
}

func participantIDs(ss stateSets[string]) (participantIDs map[string]struct{}) {
	participantIDs = make(map[string]struct{}, len(ss.prepared)+len(ss.prepareFailed)+len(ss.committed)+len(ss.rolledBack))

	for participantID := range ss.prepared {
		participantIDs[participantID] = struct{}{}
	}
	for participantID := range ss.prepareFailed {
		participantIDs[participantID] = struct{}{}
	}
	for participantID := range ss.committed {
		participantIDs[participantID] = struct{}{}
	}
	for participantID := range ss.rolledBack {
		participantIDs[participantID] = struct{}{}
	}

	return participantIDs
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
