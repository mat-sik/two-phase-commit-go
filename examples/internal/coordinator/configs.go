package coordinator

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/basic"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/transfer"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/persister"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

func NewMockPersistenceConfig() twopc.PersistenceConfig[string] {
	return twopc.PersistenceConfig[string]{
		TransactionStateChecker:   persister.MockTransactionStateChecker{},
		TransactionStatePersister: persister.MockTransactionStatePersister{},
	}
}

func NewPostgresPersistenceConfig(pool *pgxpool.Pool) twopc.PersistenceConfig[string] {
	return twopc.PersistenceConfig[string]{
		TransactionStateChecker:   persister.NewPostgresTransactionStateChecker(pool),
		TransactionStatePersister: persister.NewPostgresTransactionStatePersister(pool),
	}
}

func NewBasicGRPCClient() twopc.ClientConfig[string] {
	return twopc.ClientConfig[string]{
		NewClientFunc: basic.NewGRPCClient,
	}
}

func NewTransferGRPCClient() twopc.ClientConfig[string] {
	return twopc.ClientConfig[string]{
		NewClientFunc: transfer.NewGRPCClient,
	}
}

func NewRestClient() twopc.ClientConfig[string] {
	return twopc.ClientConfig[string]{
		NewClientFunc: client.NewRESTClient,
	}
}
