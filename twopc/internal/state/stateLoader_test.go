package state

import (
	"reflect"
	"testing"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/client"
	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

func Test_stateLoader_loadState(t *testing.T) {
	type fields struct {
		transactionStateChecker TransactionStateChecker
	}
	type args struct {
		transactionID string
		clientIDS     []client.ID
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   State
	}{
		{
			name: "all prepare failed",
			fields: fields{
				transactionStateChecker: mockTransactionStateChecker{
					stateByClientID: map[client.ID]transaction.State{
						"host-a": transaction.PrepareFailed,
						"host-b": transaction.PrepareFailed,
					},
				},
			},
			args: args{
				transactionID: "tx-1",
				clientIDS:     []client.ID{"host-a", "host-b"},
			},
			want: State{
				stateSets: stateSets{
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
					stateByClientID: map[client.ID]transaction.State{
						"host-a": transaction.Prepared,
						"host-b": transaction.Prepared,
					},
				},
			},
			args: args{
				transactionID: "tx-2",
				clientIDS:     []client.ID{"host-a", "host-b"},
			},
			want: State{
				stateSets: stateSets{
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
					stateByClientID: map[client.ID]transaction.State{
						"host-a": transaction.Prepared,
						"host-b": transaction.Prepared,
						"host-c": transaction.Committed,
					},
				},
			},
			args: args{
				transactionID: "tx-3",
				clientIDS:     []client.ID{"host-a", "host-b", "host-c"},
			},
			want: State{
				stateSets: stateSets{
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
					stateByClientID: map[client.ID]transaction.State{
						"host-a": transaction.RolledBack,
						"host-b": transaction.RolledBack,
					},
				},
			},
			args: args{
				transactionID: "tx-4",
				clientIDS:     []client.ID{"host-a", "host-b"},
			},
			want: State{
				stateSets: stateSets{
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
					stateByClientID: map[client.ID]transaction.State{},
				},
			},
			args: args{
				transactionID: "tx-5",
				clientIDS:     nil,
			},
			want: State{
				stateSets: stateSets{
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
					stateByClientID: map[client.ID]transaction.State{
						"host-a": transaction.Prepared,
						"host-z": transaction.Committed, // not client ids
					},
				},
			},
			args: args{
				transactionID: "tx-6",
				clientIDS:     []client.ID{"host-a"},
			},
			want: State{
				stateSets: stateSets{
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
			sl := Loader{
				transactionStateChecker: tt.fields.transactionStateChecker,
			}
			if got := sl.LoadState(tt.args.transactionID, tt.args.clientIDS); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LoadState() = %v, want %v", got, tt.want)
			}
		})
	}
}

type mockTransactionStateChecker struct {
	stateByClientID map[client.ID]transaction.State
}

func (m mockTransactionStateChecker) Check(_ string) map[client.ID]transaction.State {
	return m.stateByClientID
}
