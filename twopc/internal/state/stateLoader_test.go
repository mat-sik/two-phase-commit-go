package state

import (
	"reflect"
	"testing"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

func Test_stateLoader_loadState(t *testing.T) {
	type fields struct {
		transactionStateChecker TransactionStateChecker[string]
	}
	type args struct {
		transactionID string
		clientIDS     []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   State[string]
	}{
		{
			name: "all prepare failed",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByClientID: map[string]transaction.State{
						"host-a": transaction.PrepareFailed,
						"host-b": transaction.PrepareFailed,
					},
				},
			},
			args: args{
				transactionID: "tx-1",
				clientIDS:     []string{"host-a", "host-b"},
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
					stateByClientID: map[string]transaction.State{
						"host-a": transaction.Prepared,
						"host-b": transaction.Prepared,
					},
				},
			},
			args: args{
				transactionID: "tx-2",
				clientIDS:     []string{"host-a", "host-b"},
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
					stateByClientID: map[string]transaction.State{
						"host-a": transaction.Prepared,
						"host-b": transaction.Prepared,
						"host-c": transaction.Committed,
					},
				},
			},
			args: args{
				transactionID: "tx-3",
				clientIDS:     []string{"host-a", "host-b", "host-c"},
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
					stateByClientID: map[string]transaction.State{
						"host-a": transaction.RolledBack,
						"host-b": transaction.RolledBack,
					},
				},
			},
			args: args{
				transactionID: "tx-4",
				clientIDS:     []string{"host-a", "host-b"},
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
			name: "empty client ids",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByClientID: map[string]transaction.State{},
				},
			},
			args: args{
				transactionID: "tx-5",
				clientIDS:     nil,
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
			name: "checker returns state for extra hosts not in client ids — only wanted client ids are mapped",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByClientID: map[string]transaction.State{
						"host-a": transaction.Prepared,
						"host-z": transaction.Committed, // not in client ids
					},
				},
			},
			args: args{
				transactionID: "tx-6",
				clientIDS:     []string{"host-a"},
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sl := Loader[string]{
				transactionStateChecker: tt.fields.transactionStateChecker,
			}
			if got := sl.LoadState(tt.args.transactionID, tt.args.clientIDS); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LoadState() = %v, want %v", got, tt.want)
			}
		})
	}
}

type mockTransactionStateChecker struct {
	stateByClientID map[string]transaction.State
}

func (m mockTransactionStateChecker) Check(_ string) map[string]transaction.State {
	return m.stateByClientID
}
