//go:build testcontainers

package test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	client.InitLogger()

	container, err := postgres.Run(ctx,
		"postgres:17",
		postgres.WithInitScripts("testdata/coordinator-schema.sql"),
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

func cleanup() {
	if _, err := pool.Exec(context.Background(), "TRUNCATE transaction_states"); err != nil {
		slog.Error("failed to cleanup", "err", err)
	}
}
