package test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator"
	"github.com/mat-sik/two-phase-commit-go/twopc"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := postgres.Run(ctx,
		"postgres:17",
		postgres.WithInitScripts("testdata/schema.sql"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		),
	)
	if err != nil {
		slog.Error("failed to start container", "err", err)
	}
	defer func() {
		if err = container.Terminate(ctx); err != nil {
			slog.Error("failed to terminate container", "err", err)
			return
		}
	}()

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		slog.Error("failed to get connection string", "err", err)
	}

	pool, err = pgxpool.New(ctx, connStr)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	m.Run()
}

func Test_sql_integration(t *testing.T) {
	tests := []testCase{
		{
			name: "simple happy path",
			runServerRequests: client.GRPCServerRequests([]*client.GRPCHandler{
				client.NewNoopGRPCHandler(),
				client.NewNoopGRPCHandler(),
				client.NewNoopGRPCHandler(),
			}),
			txCoordinator: twopc.NewCoordinator(
				coordinator.SqlTransactionStateChecker{Pool: pool},
				coordinator.SqlStatePersister{Pool: pool},
				coordinator.NewGRPCClient,
			),
			distributedTransaction: distributedTransaction{
				transactionID: "tx-psql-1",
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			t.Cleanup(cleanup)
			runTest(t, tt)
		})
	}
}

func Test_eventual_consistency(t *testing.T) {
	t.Run("first coordinator doesn't finish, second does", func(t *testing.T) {
		t.Parallel()

		txCoordinator := twopc.NewCoordinator(
			coordinator.SqlTransactionStateChecker{Pool: pool},
			coordinator.SqlStatePersister{Pool: pool},
			coordinator.NewGRPCClient,
			twopc.WithBackoffMax(200*time.Millisecond),
		)

		tx := distributedTransaction{
			transactionID: "tx-psql-1",
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
		}

		srvConfig := client.GRPCServerRequests([]*client.GRPCHandler{
			client.NewFailingNoopGRPCHandler(0, 15, 0),
			client.NewFailingNoopGRPCHandler(0, 20, 0),
			client.NewFailingNoopGRPCHandler(0, 30, 0),
		})

		srvBundle, err := client.RunServers(srvConfig)
		if err != nil {
			t.Fatalf("failed to start servers: %v", err)
		}

		testCtx, testCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer testCancel()
		for {
			if err = testCtx.Err(); err != nil {
				t.Fatalf("failed to eventually finish in committed state, err: %v", err)
			}

			ctx, cancel := context.WithTimeout(testCtx, 1*time.Second)

			addresses := srvBundle.Addresses()
			outcome := txCoordinator.Execute(ctx, tx.toTwopc(addresses))
			cancel()
			if outcome.Outcome() == twopc.OutcomeCommitted {
				break
			}
		}

		errs := srvBundle.Shutdown()
		if len(errs) != 0 {
			t.Errorf("got %d server errors: %v", len(errs), errs)
		}
	})
}

func cleanup() {
	if _, err := pool.Exec(context.Background(), "TRUNCATE transaction_states"); err != nil {
		slog.Error("failed to cleanup", "err", err)
	}
}
