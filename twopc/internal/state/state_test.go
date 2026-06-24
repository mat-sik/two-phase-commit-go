package state

import (
	"reflect"
	"sort"
	"testing"
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
		// ═════════════════════════════════════════════════════════════════════════════
		// GROUP 0 - single host
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name: "(NS) -prepare(ok)-> (P)",
			state: State[string]{
				participantIDs: hosts("host-a"),
				stateSets:      baseStateSets(),
			},
			args: args{
				successfulTransitions: trs(PrepareTransition("host-a")),
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "(NS) -prepare(fail)-> (PF)",
			state: State[string]{
				participantIDs: hosts("host-a"),
				stateSets:      baseStateSets(),
			},
			args: args{
				failedTransitions: trs(PrepareTransition("host-a")),
			},
			want: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "(P) -commit(ok)-> (C)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				successfulTransitions: trs(CommitTransition("host-a")),
			},
			want: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "(P) -commit(fail)-> (P)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				failedTransitions: trs(CommitTransition("host-a")),
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "(P) -rollback(ok)-> (R)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				successfulTransitions: trs(RollbackTransition("host-a")),
			},
			want: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a"),
			}),
		},
		{
			name: "(P) -rollback(fail)-> (P)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				failedTransitions: trs(RollbackTransition("host-a")),
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		// ═════════════════════════════════════════════════════════════════════════════
		// GROUP 1 – (NS, NS)
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name:  "(NS:a,b) -prepare(ok)-> (P:a,b)",
			state: newNotStartedState("host-a", "host-b"),
			args: args{
				successfulTransitions: trs(
					PrepareTransition("host-a"),
					PrepareTransition("host-b"),
				),
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name:  "(NS:a,b) -prepare(ok:a,fail:b)-> (P:a,PF:b)",
			state: newNotStartedState("host-a", "host-b"),
			args: args{
				successfulTransitions: trs(
					PrepareTransition("host-a"),
				),
				failedTransitions: trs(
					PrepareTransition("host-b"),
				),
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name:  "(NS:a,b) -prepare(fail)-> (PF:a,b)",
			state: newNotStartedState("host-a", "host-b"),
			args: args{
				failedTransitions: trs(
					PrepareTransition("host-a"),
					PrepareTransition("host-b"),
				),
			},
			want: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a", "host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		// ═════════════════════════════════════════════════════════════════════════════
		// GROUP 2 – (NS, P)
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name: "(NS:a,P:b) -prepare(ok:a)-> (P:a,b)",
			state: State[string]{
				participantIDs: hosts("host-a", "host-b"),
				stateSets: stateSets[string]{
					prepared:      hosts("host-b"),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
			},
			args: args{
				successfulTransitions: trs(
					PrepareTransition("host-a"),
				),
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "(NS:a,P:b) -prepare(fail:a)-> (P:b,PF:a)",
			state: State[string]{
				participantIDs: hosts("host-a", "host-b"),
				stateSets: stateSets[string]{
					prepared:      hosts("host-b"),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
			},
			args: args{
				failedTransitions: trs(
					PrepareTransition("host-a"),
				),
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-b"),
				prepareFailed: hosts("host-a"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		// ═════════════════════════════════════════════════════════════════════════════
		// GROUP 3 – (P, P)
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name: "(P:a,b) -commit(ok)-> (C:a,b)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				successfulTransitions: trs(
					CommitTransition("host-a"),
					CommitTransition("host-b"),
				),
			},
			want: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a", "host-b"),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "(P:a,b) -commit(ok:a,fail:b)-> (P:b,C:a)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				successfulTransitions: trs(
					CommitTransition("host-a"),
				),
				failedTransitions: trs(
					CommitTransition("host-b"),
				),
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-b"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "(P:a,b) -commit(fail)-> (P:a,b)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				failedTransitions: trs(
					CommitTransition("host-a"),
					CommitTransition("host-b"),
				),
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		// ═════════════════════════════════════════════════════════════════════════════
		// GROUP 4 – (P, PF)
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name: "(P:a,PF:b) -rollback(ok:a)-> (PF:b,R:a)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				successfulTransitions: trs(
					RollbackTransition("host-a"),
				),
			},
			want: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a"),
			}),
		},
		{
			name: "(P:a,PF:b) -rollback(fail:a)-> (P:a,PF:b)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				failedTransitions: trs(
					RollbackTransition("host-a"),
				),
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		// ═════════════════════════════════════════════════════════════════════════════
		// GROUP 5 – (P, C)
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name: "(P:a,C:b) -commit(ok:a)-> (C:a,b)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-b"),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				successfulTransitions: trs(
					CommitTransition("host-a"),
				),
			},
			want: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a", "host-b"),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "(P:a,C:b) -commit(fail:a)-> (P:a,C:b)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-b"),
				rolledBack:    emptyHosts(),
			}),
			args: args{
				failedTransitions: trs(
					CommitTransition("host-a"),
				),
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-b"),
				rolledBack:    emptyHosts(),
			}),
		},
		// ═════════════════════════════════════════════════════════════════════════════
		// GROUP 7 – (P, R)
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name: "(P:a,R:b) -rollback(ok:a)-> (R:a,b)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-b"),
			}),
			args: args{
				successfulTransitions: trs(
					RollbackTransition("host-a"),
				),
			},
			want: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a", "host-b"),
			}),
		},
		{
			name: "(P:a,R:b) -rollback(fail:a)-> (P:a,R:b)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-b"),
			}),
			args: args{
				failedTransitions: trs(
					RollbackTransition("host-a"),
				),
			},
			want: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-b"),
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
		{
			name: "(NS)",
			state: State[string]{
				participantIDs: hosts("host-a"),
				stateSets:      baseStateSets(),
			},
			want: trs(
				PrepareTransition("host-a"),
			),
		},
		{
			name: "(P)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			want: trs(
				CommitTransition("host-a"),
			),
		},
		{
			name:  "(NS,NS)",
			state: newNotStartedState("host-a", "host-b"),
			want: trs(
				PrepareTransition("host-a"),
				PrepareTransition("host-b"),
			),
		},
		{
			name: "(NS, P)",
			state: State[string]{
				participantIDs: hosts("host-a", "host-b"),
				stateSets: stateSets[string]{
					prepared:      hosts("host-b"),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
			},
			want: trs(
				PrepareTransition("host-a"),
			),
		},
		{

			name: "(P,P)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a", "host-b"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			want: trs(
				CommitTransition("host-a"),
				CommitTransition("host-b"),
			),
		},
		{
			name: "(P,PF)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: hosts("host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
			want: trs(
				RollbackTransition("host-a"),
			),
		},
		{
			name: "(P,C)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-b"),
				rolledBack:    emptyHosts(),
			}),
			want: trs(
				CommitTransition("host-a"),
			),
		},
		{

			name: "(P,R)",
			state: newState(stateSets[string]{
				prepared:      hosts("host-a"),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-b"),
			}),
			want: trs(
				RollbackTransition("host-a"),
			),
		},
		// ═════════════════════════════════════════════════════════════════════════════
		// Terminal states
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name: "(PF)",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "(C)",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "(R)",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a"),
			}),
		},
		{
			name: "(NS, C)",
			state: State[string]{
				participantIDs: hosts("host-a", "host-b"),
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: emptyHosts(),
					committed:     hosts("host-b"),
					rolledBack:    emptyHosts(),
				},
			},
		},
		{
			name: "(NS,PF)",
			state: State[string]{
				participantIDs: hosts("host-a", "host-b"),
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: hosts("host-b"),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
			},
		},
		{
			name: "(NS, R)",
			state: State[string]{
				participantIDs: hosts("host-a", "host-b"),
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    hosts("host-b"),
				},
			},
		},
		{
			name: "(PF,PF)",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a", "host-b"),
				committed:     emptyHosts(),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "(PF,R)",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-a"),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-c"),
			}),
		},
		{
			name: "(C,C)",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a", "host-b"),
				rolledBack:    emptyHosts(),
			}),
		},
		{
			name: "(R,R)",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     emptyHosts(),
				rolledBack:    hosts("host-a", "host-b"),
			}),
		},
		// ═════════════════════════════════════════════════════════════════════════════
		// Invalid states
		// ═════════════════════════════════════════════════════════════════════════════
		{
			name: "(C,NS)",
			state: State[string]{
				participantIDs: hosts("host-a", "host-b"),
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: emptyHosts(),
					committed:     hosts("host-a"),
					rolledBack:    emptyHosts(),
				},
			},
			wantErr: true,
		},
		{
			name: "(C,PF)",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: hosts("host-b"),
				committed:     hosts("host-a"),
				rolledBack:    emptyHosts(),
			}),
			wantErr: true,
		},
		{
			name: "(C,R)",
			state: newState(stateSets[string]{
				prepared:      emptyHosts(),
				prepareFailed: emptyHosts(),
				committed:     hosts("host-a"),
				rolledBack:    hosts("host-b"),
			}),
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

func baseStateSets() stateSets[string] {
	return stateSets[string]{
		prepared:      emptyHosts(),
		prepareFailed: emptyHosts(),
		committed:     emptyHosts(),
		rolledBack:    emptyHosts(),
	}
}

func newNotStartedState(notStarted ...string) State[string] {
	return State[string]{
		participantIDs: hosts(notStarted...),
		stateSets: stateSets[string]{
			prepared:      emptyHosts(),
			prepareFailed: emptyHosts(),
			committed:     emptyHosts(),
			rolledBack:    emptyHosts(),
		},
	}
}

func newState(ss stateSets[string]) State[string] {
	return State[string]{
		participantIDs: participantIDs(ss),
		stateSets:      ss,
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

func trs(transitions ...Transition[string]) []Transition[string] {
	return transitions
}
