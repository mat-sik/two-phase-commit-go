package state

import (
	"errors"
	"reflect"
	"testing"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

func Test_stateLoader_loadState(t *testing.T) {
	type fields struct {
		transactionStateChecker TransactionStateChecker[string]
	}
	type args struct {
		transactionID  string
		participantIDs []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    State[string]
		wantErr error
	}{
		{
			name: "all prepare failed",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByParticipantID: map[string]transaction.State{
						"host-a": transaction.PrepareFailed,
						"host-b": transaction.PrepareFailed,
					},
				},
			},
			args: args{
				transactionID:  "tx-1",
				participantIDs: []string{"host-a", "host-b"},
			},
			want: State[string]{
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: hosts("host-a", "host-b"),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
			},
		},
		{
			name: "all prepared",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByParticipantID: map[string]transaction.State{
						"host-a": transaction.Prepared,
						"host-b": transaction.Prepared,
					},
				},
			},
			args: args{
				transactionID:  "tx-2",
				participantIDs: []string{"host-a", "host-b"},
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
			name: "mixed states across hosts",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByParticipantID: map[string]transaction.State{
						"host-a": transaction.Prepared,
						"host-b": transaction.Prepared,
						"host-c": transaction.Committed,
					},
				},
			},
			args: args{
				transactionID:  "tx-3",
				participantIDs: []string{"host-a", "host-b", "host-c"},
			},
			want: State[string]{
				stateSets: stateSets[string]{
					prepared:      hosts("host-a", "host-b"),
					prepareFailed: emptyHosts(),
					committed:     hosts("host-c"),
					rolledBack:    emptyHosts(),
				},
			},
		},
		{
			name: "all rolled back",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByParticipantID: map[string]transaction.State{
						"host-a": transaction.RolledBack,
						"host-b": transaction.RolledBack,
					},
				},
			},
			args: args{
				transactionID:  "tx-4",
				participantIDs: []string{"host-a", "host-b"},
			},
			want: State[string]{
				stateSets: stateSets[string]{
					prepared:      emptyHosts(),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    hosts("host-a", "host-b"),
				},
			},
		},
		{
			name: "empty participant ids",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByParticipantID: map[string]transaction.State{},
				},
			},
			args: args{
				transactionID:  "tx-5",
				participantIDs: nil,
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
			name: "checker returns state for extra hosts not in participant ids — only wanted client ids are mapped",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByParticipantID: map[string]transaction.State{
						"host-a": transaction.Prepared,
						"host-z": transaction.Committed, // not in participant ids
					},
				},
			},
			args: args{
				transactionID:  "tx-6",
				participantIDs: []string{"host-a"},
			},
			want: State[string]{
				stateSets: stateSets[string]{
					prepared:      hosts("host-a"),
					prepareFailed: emptyHosts(),
					committed:     emptyHosts(),
					rolledBack:    emptyHosts(),
				},
			},
		},
		{
			name: "checker returns err on loading state",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByParticipantID: map[string]transaction.State{},
					err:                  errAny,
				},
			},
			args:    args{},
			want:    State[string]{},
			wantErr: errAny,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sl := Loader[string]{
				transactionStateChecker: tt.fields.transactionStateChecker,
			}
			got, err := sl.LoadState(tt.args.transactionID, tt.args.participantIDs)
			if errors.Is(tt.wantErr, errAny) && err == nil {
				t.Errorf("LoadState() expected err, but got no err")
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LoadState() = %v, want %v", got, tt.want)
			}
		})
	}
}

var errAny = errors.New("any error")

type mockTransactionStateChecker struct {
	stateByParticipantID map[string]transaction.State
	err                  error
}

func (m mockTransactionStateChecker) Check(_ string) (map[string]transaction.State, error) {
	return m.stateByParticipantID, m.err
}
