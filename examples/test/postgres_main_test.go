//go:build testcontainers

package test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var createDatabaseFunc databaseCreator

func TestMain(m *testing.M) {
	const function = "TestMain"

	ctx := context.Background()

	client.InitLogger()

	container, err := postgres.Run(ctx,
		"postgres:17",
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
		slog.Error("failed to start container", "function", function, "err", err)
	}
	defer func() {
		if err = container.Terminate(ctx); err != nil {
			slog.Error("failed to terminate container", "function", function, "err", err)
			return
		}
	}()

	var connStr string
	connStr, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		slog.Error("failed to get admin connection string", "function", function, "err", err)
	}

	var adminPool *pgxpool.Pool
	adminPool, err = pgxpool.New(ctx, connStr)
	if err != nil {
		panic(err)
	}
	defer adminPool.Close()

	createDatabaseFunc = newDatabaseCreator(adminPool, container)

	m.Run()
}

type databaseCreator func(ctx context.Context, dbName string, scripts ...string) (*pgxpool.Pool, databaseDropper)

func newDatabaseCreator(adminPool *pgxpool.Pool, container *postgres.PostgresContainer) databaseCreator {
	return func(ctx context.Context, dbName string, scripts ...string) (*pgxpool.Pool, databaseDropper) {
		pool := createDatabase(ctx, adminPool, container, dbName, scripts...)
		return pool, newDatabaseDropper(adminPool, pool, dbName)
	}
}

func createDatabase(ctx context.Context, adminPool *pgxpool.Pool, container *postgres.PostgresContainer, dbName string, scripts ...string) *pgxpool.Pool {
	const function = "createDatabase"

	createDatabaseQuery := fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)
	if _, err := adminPool.Exec(ctx, createDatabaseQuery); err != nil {
		panic(fmt.Sprintf("%s: %v", function, err))
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable", "dbname="+dbName)
	if err != nil {
		panic(fmt.Sprintf("%s: new connections tring: %v", function, err))
	}

	var pool *pgxpool.Pool
	pool, err = pgxpool.New(ctx, connStr)
	if err != nil {
		panic(fmt.Sprintf("%s: new pool: %v", function, err))
	}

	for _, scriptPath := range scripts {
		var sqlContent []byte
		sqlContent, err = os.ReadFile(scriptPath)
		if err != nil {
			pool.Close()
			panic(fmt.Sprintf("%s: failed to read schema script %s: %v", function, scriptPath, err))
		}

		if _, err = pool.Exec(ctx, string(sqlContent)); err != nil {
			pool.Close()
			panic(fmt.Sprintf("%s: failed to execute schema %s: %v", function, scriptPath, err))
		}
	}

	return pool
}

type databaseDropper func()

func newDatabaseDropper(adminPool *pgxpool.Pool, pool *pgxpool.Pool, dbName string) databaseDropper {
	return func() {
		dropDatabase(adminPool, pool, dbName)
	}
}

func dropDatabase(adminPool *pgxpool.Pool, pool *pgxpool.Pool, dbName string) {
	const function = "dropDatabase"

	pool.Close()

	dropDatabaseQuery := fmt.Sprintf(`DROP DATABASE "%s"`, dbName)
	if _, err := adminPool.Exec(context.Background(), dropDatabaseQuery); err != nil {
		panic(fmt.Sprintf("%s: %v", function, err))
	}
}
