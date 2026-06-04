package coordinator

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func NewMockRESTCoordinator(opts ...twopc.Option) *twopc.Coordinator[string] {
	return twopc.NewCoordinator(
		newMockPersistenceConfig(),
		newRESTClientConfig(),
		opts...,
	)
}

func NewMockGRPCCoordinator(opts ...twopc.Option) *twopc.Coordinator[string] {
	return twopc.NewCoordinator(
		newMockPersistenceConfig(),
		newGRPCClientConfig(),
		opts...,
	)
}

func NewSQLRESTCoordinator(pool *pgxpool.Pool, opts ...twopc.Option) *twopc.Coordinator[string] {
	return twopc.NewCoordinator(
		newSQLPersistenceConfig(pool),
		newRESTClientConfig(),
		opts...,
	)
}

func NewSQLGRPCCoordinator(pool *pgxpool.Pool, opts ...twopc.Option) *twopc.Coordinator[string] {
	return twopc.NewCoordinator(
		newSQLPersistenceConfig(pool),
		newGRPCClientConfig(),
		opts...,
	)
}

func newMockPersistenceConfig() twopc.PersistenceConfig[string] {
	return twopc.PersistenceConfig[string]{
		TransactionStateChecker:   MockTransactionStateChecker{},
		TransactionStatePersister: MockTransactionStatePersister{},
	}
}

func newSQLPersistenceConfig(pool *pgxpool.Pool) twopc.PersistenceConfig[string] {
	return twopc.PersistenceConfig[string]{
		TransactionStateChecker:   SqlTransactionStateChecker{Pool: pool},
		TransactionStatePersister: SqlTransactionStatePersister{Pool: pool},
	}
}

func newRESTClientConfig() twopc.ClientConfig[string] {
	return twopc.ClientConfig[string]{
		NewClientFunc: NewRESTClient,
	}
}

func newGRPCClientConfig() twopc.ClientConfig[string] {
	return twopc.ClientConfig[string]{
		NewClientFunc: NewGRPCClient,
	}
}
