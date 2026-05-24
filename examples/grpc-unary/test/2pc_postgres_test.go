package test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/grpc-unary/internal/client"
	"github.com/mat-sik/two-phase-commit-go/examples/grpc-unary/internal/coordinator"
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
	tests := []struct {
		name          string
		serverConfigs []serverConfig
		txCoordinator *twopc.Coordinator[string]
		request       twopc.DistributedTransaction[string]
		wantErr       bool
		wantedOutcome twopc.Outcome
	}{
		{
			name: "simple happy path",
			serverConfigs: []serverConfig{
				{
					port:    30050,
					handler: client.NewNoopHandler(),
				},
				{
					port: 30051, handler: client.NewNoopHandler(),
				},
				{
					port:    30052,
					handler: client.NewNoopHandler(),
				},
			},
			txCoordinator: twopc.NewCoordinator(
				coordinator.SqlTransactionStateChecker{Pool: pool},
				coordinator.SqlStatePersister{Pool: pool},
				coordinator.NewGRPCClient,
			),
			request: twopc.DistributedTransaction[string]{
				TransactionID: "tx-1",
				Transactions: []twopc.Transaction[string]{
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30050),
						Payload:       "one",
					},
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30051),
						Payload:       "two",
					},
					{
						ParticipantID: fmt.Sprintf("localhost:%d", 30052),
						Payload:       "three",
					},
				},
			},
			wantErr:       false,
			wantedOutcome: twopc.OutcomeCommitted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Cleanup(cleanup)

			srvBundle, err := runServers(tt.serverConfigs)
			if err != nil {
				t.Fatalf("failed to start servers: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			outcome := tt.txCoordinator.Execute(ctx, tt.request)
			if tt.wantedOutcome != outcome.Outcome() {
				t.Fatalf("expected outcome %v, got %v", tt.wantedOutcome, outcome.Outcome())
			}
			if tt.wantErr && outcome.Err() == nil {
				t.Fatalf("expected error")
			}
			if !tt.wantErr && outcome.Err() != nil {
				t.Fatalf("didn't expect error, got %v", outcome.Err())
			}

			for _, server := range srvBundle.servers {
				go server.GracefulStop()
			}

			var errs []error
			for err = range srvBundle.serverErrsChan {
				if err != nil {
					errs = append(errs, err)
				}
			}
			if len(errs) != 0 {
				t.Errorf("got %d server errors: %v", len(errs), errs)
			}
		})
	}
}

func cleanup() {
	if _, err := pool.Exec(context.Background(), "TRUNCATE transaction_states"); err != nil {
		slog.Error("failed to cleanup", "err", err)
	}
}
