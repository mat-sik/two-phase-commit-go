package migrations

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/pressly/goose/v3"
)

func Run(pool *pgxpool.Pool, dir string) error {
	sqlDB := stdlib.OpenDBFromPool(pool)
	defer func() {
		if err := sqlDB.Close(); err != nil {
			slog.Error("closing sql DB while migrating schema")
		}
	}()

	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}

	return goose.Up(sqlDB, dir)
}
