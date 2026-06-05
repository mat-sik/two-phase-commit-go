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
	ctx := context.Background()

	client.InitLogger()

	container, err := postgres.Run(ctx,
		"postgres:17",
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

	var connStr string
	connStr, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		slog.Error("failed to get admin connection string", "err", err)
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
	const method = "createDatabase"

	createDatabaseQuery := "CREATE DATABASE " + dbName
	if _, err := adminPool.Exec(ctx, createDatabaseQuery); err != nil {
		panic(fmt.Sprintf("%s: %v", method, err))
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable", "dbname="+dbName)
	if err != nil {
		panic(fmt.Sprintf("%s: new connections tring: %v", method, err))
	}

	var pool *pgxpool.Pool
	pool, err = pgxpool.New(ctx, connStr)
	if err != nil {
		panic(fmt.Sprintf("%s: new pool: %v", method, err))
	}

	for _, scriptPath := range scripts {
		var sqlContent []byte
		sqlContent, err = os.ReadFile(scriptPath)
		if err != nil {
			pool.Close()
			panic(fmt.Sprintf("%s: failed to read schema script %s: %v", method, scriptPath, err))
		}

		if _, err = pool.Exec(ctx, string(sqlContent)); err != nil {
			pool.Close()
			panic(fmt.Sprintf("%s: failed to execute schema %s: %v", method, scriptPath, err))
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
	const method = "dropDatabase"

	pool.Close()

	dropDatabaseQuery := "DROP DATABASE " + dbName
	if _, err := adminPool.Exec(context.Background(), dropDatabaseQuery); err != nil {
		panic(fmt.Sprintf("%s: %v", method, err))
	}
}
