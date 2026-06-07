//go:build testcontainers

package test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestMain(m *testing.M) {
	client.InitLogger()
	m.Run()
}

func runPostgresForParticipantPool(ctx context.Context) (*pgxpool.Pool, postgresTerminator, error) {
	// TODO: rename client-schema to participant-schema
	return runPostgresForPool(ctx, "testdata/client-schema.sql")
}

func runPostgresForCoordinatorPool(ctx context.Context) (*pgxpool.Pool, postgresTerminator, error) {
	return runPostgresForPool(ctx, "testdata/coordinator-schema.sql")
}

func runPostgresForPool(ctx context.Context, scripts ...string) (*pgxpool.Pool, postgresTerminator, error) {
	const function = "runPostgresAndGetNewPool"

	container, err := runPostgres(ctx, scripts...)
	if err != nil {
		return nil, nil, err
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, nil, fmt.Errorf("%s: failed to get connection string: %v", function, err)
	}

	slog.Info("obtained new connection", "function", function, "connections string", connStr)

	var pool *pgxpool.Pool
	pool, err = pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: failed to create new pgxpool: %v", function, err)
	}

	terminator := newPostgresTerminator(pool, container)

	return pool, terminator, nil
}

func runPostgres(ctx context.Context, scripts ...string) (*postgres.PostgresContainer, error) {
	const function = "runPostgres"

	container, err := postgres.Run(ctx,
		"postgres:17",
		postgres.WithInitScripts(scripts...),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		),
		testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
			req.Cmd = []string{
				"postgres",
				"-c", "fsync=off",
				"-c", "max_prepared_transactions=100",
			}
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to run container: %v", function, err)
	}

	slog.Info("started postgres container", "function", function, "scripts", scripts)

	return container, nil
}

type postgresTerminator func()

func newPostgresTerminator(pool *pgxpool.Pool, container *postgres.PostgresContainer) postgresTerminator {
	const function = "newPostgresTerminator"

	return func() {
		pool.Close()

		if err := container.Terminate(context.Background()); err != nil {
			panic(fmt.Sprintf("%s: failed to terminate container: %v", function, err))
		}

		slog.Info("terminated postgres container", "function", function)
	}
}
