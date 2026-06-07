package coordinator

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/basic"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/transfer"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/persister"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func NewMockBasicRESTCoordinator(opts ...twopc.Option) *twopc.Coordinator[string] {
	return twopc.NewCoordinator(
		newMockPersistenceConfig(),
		newRestClient(),
		opts...,
	)
}

func NewMockBasicGRPCCoordinator(opts ...twopc.Option) *twopc.Coordinator[string] {
	return twopc.NewCoordinator(
		newMockPersistenceConfig(),
		newBasicGRPCClient(),
		opts...,
	)
}

func NewPostgresBasicRESTCoordinator(pool *pgxpool.Pool, opts ...twopc.Option) *twopc.Coordinator[string] {
	return twopc.NewCoordinator(
		newPostgresPersistenceConfig(pool),
		newRestClient(),
		opts...,
	)
}

func NewPostgresBasicGRPCCoordinator(pool *pgxpool.Pool, opts ...twopc.Option) *twopc.Coordinator[string] {
	return twopc.NewCoordinator(
		newPostgresPersistenceConfig(pool),
		newBasicGRPCClient(),
		opts...,
	)
}

func NewPostgresTransferGRPCCoordinator(pool *pgxpool.Pool, opts ...twopc.Option) *twopc.Coordinator[string] {
	return twopc.NewCoordinator(
		newPostgresPersistenceConfig(pool),
		newTransferGRPCClient(),
		opts...,
	)
}

func NewPostgresTransferRESTCoordinator(pool *pgxpool.Pool, opts ...twopc.Option) *twopc.Coordinator[string] {
	return twopc.NewCoordinator(
		newPostgresPersistenceConfig(pool),
		newRestClient(),
		opts...,
	)
}

func newMockPersistenceConfig() twopc.PersistenceConfig[string] {
	return twopc.PersistenceConfig[string]{
		TransactionStateChecker:   persister.MockTransactionStateChecker{},
		TransactionStatePersister: persister.MockTransactionStatePersister{},
	}
}

func newPostgresPersistenceConfig(pool *pgxpool.Pool) twopc.PersistenceConfig[string] {
	return twopc.PersistenceConfig[string]{
		TransactionStateChecker:   persister.NewPostgresTransactionStateChecker(pool),
		TransactionStatePersister: persister.NewPostgresTransactionStatePersister(pool),
	}
}

func newBasicGRPCClient() twopc.ClientConfig[string] {
	return twopc.ClientConfig[string]{
		NewClientFunc: basic.NewGRPCClient,
	}
}

func newTransferGRPCClient() twopc.ClientConfig[string] {
	return twopc.ClientConfig[string]{
		NewClientFunc: transfer.NewGRPCClient,
	}
}

func newRestClient() twopc.ClientConfig[string] {
	return twopc.ClientConfig[string]{
		NewClientFunc: client.NewRESTClient,
	}
}
