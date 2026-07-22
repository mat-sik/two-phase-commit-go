//go:build testcontainers

package test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/twopc"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type testContainersTestCase struct {
	name                   string
	serverSpecs            []serverSpec
	coordinatorConfig      testContainersCoordinatorConfig
	distributedTransaction distributedTransaction
	wantErr                bool
	wantedOutcome          twopc.Outcome
}

type testContainersCoordinatorConfig struct {
	persistenceConfigProvider persistenceConfigProvider
	opts                      []twopc.Option
}

type persistenceConfigProvider func(pool *pgxpool.Pool) twopc.PersistenceConfig[string]

func runTestContainersTest(t *testing.T, tt testContainersTestCase) {
	t.Helper()

	var poolNeedingParticipants []int
	for i, spec := range tt.serverSpecs {
		if _, ok := spec.(serverSpecWithPool); ok {
			poolNeedingParticipants = append(poolNeedingParticipants, i)
		}
	}

	coordinatorPool, participantPools := runPostgresForPools(t, len(poolNeedingParticipants))

	if len(poolNeedingParticipants) != len(participantPools) {
		panic("not enough participant pools")
	}
	for i, idx := range poolNeedingParticipants {
		tt.serverSpecs[idx].(serverSpecWithPool).injectPool(participantPools[i])
	}

	srvBundle, err := runServers(toServerLaunches(tt.serverSpecs))
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}

	addresses := srvBundle.addresses()

	clientConfig := twopc.ClientConfig[string]{
		NewClientFunc: newClientFunc(tt.distributedTransaction.transactions, addresses),
	}

	txCoordinator := twopc.NewCoordinator(
		tt.coordinatorConfig.persistenceConfigProvider(coordinatorPool),
		clientConfig,
		tt.coordinatorConfig.opts...,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	outcome := txCoordinator.Execute(ctx, tt.distributedTransaction.toTwopc(addresses))

	assertOutcome(t, tt.wantErr, tt.wantedOutcome, outcome)

	errs := srvBundle.shutdown()
	if len(errs) > 0 {
		t.Errorf("got %d server errors: %v", len(errs), errs)
	}
}

func runPostgresForPools(t *testing.T, participantAmount int) (*pgxpool.Pool, []*pgxpool.Pool) {
	t.Helper()

	coordinatorCh := make(chan runContainerResult)
	go func() {
		pool, terminator, err := runPostgresForCoordinatorPool(t.Context())
		coordinatorCh <- runContainerResult{
			pool:       pool,
			terminator: terminator,
			err:        err,
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(participantAmount)
	participantCh := make(chan runContainerResult, participantAmount)
	for range participantAmount {
		go func() {
			defer wg.Done()
			pool, terminator, err := runPostgresForParticipantPool(t.Context())
			participantCh <- runContainerResult{
				pool:       pool,
				terminator: terminator,
				err:        err,
			}
		}()
	}
	go func() {
		wg.Wait()
		close(participantCh)
	}()

	var errs []error

	var coordinatorPool *pgxpool.Pool
	if coordinatorResult := <-coordinatorCh; coordinatorResult.err != nil {
		errs = append(errs, coordinatorResult.err)
	} else {
		coordinatorPool = coordinatorResult.pool
		t.Cleanup(coordinatorResult.terminator)
	}

	var participantPools []*pgxpool.Pool
	for participantResult := range participantCh {
		if participantResult.err != nil {
			errs = append(errs, participantResult.err)
			continue
		}
		participantPools = append(participantPools, participantResult.pool)
		t.Cleanup(participantResult.terminator)
	}

	if len(errs) > 0 {
		t.Fatalf("failed to run required amount of postgres containers: %v", errors.Join(errs...))
	}

	return coordinatorPool, participantPools
}

type runContainerResult struct {
	pool       *pgxpool.Pool
	terminator postgresTerminator
	err        error
}

func runPostgresForParticipantPool(ctx context.Context) (*pgxpool.Pool, postgresTerminator, error) {
	return runPostgresForPool(ctx, "../db/participant/transfer/migrations/1_schema.sql")
}

func runPostgresForCoordinatorPool(ctx context.Context) (*pgxpool.Pool, postgresTerminator, error) {
	return runPostgresForPool(ctx, "../db/coordinator/migrations/1_schema.sql")
}

func runPostgresForPool(ctx context.Context, scripts ...string) (*pgxpool.Pool, postgresTerminator, error) {
	container, err := runPostgres(ctx, scripts...)
	if err != nil {
		return nil, nil, err
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, nil, fmt.Errorf("obtaining psql conn str: %w", err)
	}

	var pool *pgxpool.Pool
	pool, err = pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, nil, fmt.Errorf("creating new pool: %w", err)
	}

	terminator := newPostgresTerminator(pool, container)

	return pool, terminator, nil
}

func runPostgres(ctx context.Context, scripts ...string) (*postgres.PostgresContainer, error) {
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
		return nil, fmt.Errorf("running psql container: %w", err)
	}

	return container, nil
}

type postgresTerminator func()

func newPostgresTerminator(pool *pgxpool.Pool, container *postgres.PostgresContainer) postgresTerminator {
	return func() {
		pool.Close()

		if err := container.Terminate(context.Background()); err != nil {
			panic(fmt.Sprintf("terminating psql container: %s", err))
		}
	}
}
