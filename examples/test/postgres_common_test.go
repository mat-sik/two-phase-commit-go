//go:build testcontainers

package test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/adapter"
	"github.com/mat-sik/two-phase-commit-go/twopc"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type testContainersTestCase struct {
	name                   string
	serverRunners          []serverRunnable
	coordinatorConfig      testContainersCoordinatorConfig
	distributedTransaction distributedTransaction
	wantErr                bool
	wantedOutcome          twopc.Outcome
}

type serverRunnableWithPool interface {
	serverRunnable
	needsPool() bool
	injectPool(pool *pgxpool.Pool)
}

type serverRunnable interface {
	toRunServerRequest() runServerRequest
}

type restServerRunnable struct {
	handler  *http.ServeMux
	provider func(pool *pgxpool.Pool) *http.ServeMux
	mapper   func(mux *http.ServeMux) runServerRequest
	pool     *pgxpool.Pool
}

func newRESTHandlerServerRunnable() *restServerRunnable {
	return &restServerRunnable{
		handler: adapter.NewBasicMux(),
		mapper:  mapFromMux,
	}
}

func newRESTProviderServerRunnable() *restServerRunnable {
	return &restServerRunnable{
		provider: func(pool *pgxpool.Pool) *http.ServeMux {
			return adapter.NewTransferMux(pool)
		},
		mapper: mapFromMux,
	}
}

func (r *restServerRunnable) toRunServerRequest() runServerRequest {
	return genericToRunServerRequest(r.pool, r.handler, r.provider, r.mapper)
}

func (r *restServerRunnable) needsPool() bool {
	return r.provider != nil
}

func (r *restServerRunnable) injectPool(pool *pgxpool.Pool) {
	r.pool = pool
}

type gRPCBasicLogicServerRunnable struct {
	handler *adapter.GRPCBasicHandler
	mapper  func(mux *adapter.GRPCBasicHandler) runServerRequest
}

func newGRPCBasicLogicServerRunnable() gRPCBasicLogicServerRunnable {
	return gRPCBasicLogicServerRunnable{
		handler: adapter.NewBasicGRPCHandler(),
		mapper:  mapFromGRPCBasicHandler,
	}
}

func (r gRPCBasicLogicServerRunnable) toRunServerRequest() runServerRequest {
	return genericToRunServerRequest(nil, r.handler, nil, r.mapper)
}

type gRPCTransferLogicServerRunnable struct {
	provider func(pool *pgxpool.Pool) *adapter.GRPCTransferHandler
	mapper   func(mux *adapter.GRPCTransferHandler) runServerRequest
	pool     *pgxpool.Pool
}

func newGRPCTransferLogicServerRunnable() *gRPCTransferLogicServerRunnable {
	return &gRPCTransferLogicServerRunnable{
		provider: func(pool *pgxpool.Pool) *adapter.GRPCTransferHandler {
			return adapter.NewTransferGRPCHandler(pool)
		},
		mapper: mapFromGRPCTransferHandler,
	}
}

func (r *gRPCTransferLogicServerRunnable) toRunServerRequest() runServerRequest {
	return genericToRunServerRequest(r.pool, nil, r.provider, r.mapper)
}

func (r *gRPCTransferLogicServerRunnable) needsPool() bool {
	return true
}

func (r *gRPCTransferLogicServerRunnable) injectPool(pool *pgxpool.Pool) {
	r.pool = pool
}

func genericToRunServerRequest[T comparable](
	pool *pgxpool.Pool,
	handler T,
	provider func(*pgxpool.Pool) T,
	mapper func(T) runServerRequest,
) runServerRequest {
	var zero T
	if handler == zero && provider == nil {
		panic("server runnable handler and provider cannot be both nil")
	}
	if handler == zero {
		handler = provider(pool)
	}
	return mapper(handler)
}

type testContainersCoordinatorConfig struct {
	persistenceConfigProvider persistenceConfigProvider
	clientConfigProvider      clientConfigProvider
	opts                      []twopc.Option
}

type persistenceConfigProvider func(pool *pgxpool.Pool) twopc.PersistenceConfig[string]

func runTestContainersTest(t *testing.T, tt testContainersTestCase) {
	t.Helper()

	var poolNeedingParticipants []int
	for i, runnable := range tt.serverRunners {
		if r, ok := runnable.(serverRunnableWithPool); ok && r.needsPool() {
			poolNeedingParticipants = append(poolNeedingParticipants, i)
		}
	}

	coordinatorPool, participantPools := runPostgresForPools(t, len(poolNeedingParticipants))

	if len(poolNeedingParticipants) != len(participantPools) {
		panic("not enough participant pools")
	}
	for i, idx := range poolNeedingParticipants {
		tt.serverRunners[idx].(serverRunnableWithPool).injectPool(participantPools[i])
	}

	srvBundle, err := runServers(toRunServerRequests(tt.serverRunners))
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	participantTransports := newParticipantTransports(tt.distributedTransaction.transactions)

	addresses := srvBundle.addresses()
	txCoordinator := newCoordinator(
		tt.coordinatorConfig.persistenceConfigProvider(coordinatorPool),
		tt.coordinatorConfig.clientConfigProvider,
		participantTransports,
		addresses,
		tt.coordinatorConfig.opts...,
	)

	outcome := txCoordinator.Execute(ctx, tt.distributedTransaction.toTwopc(addresses))

	assertOutcome(t, tt.wantErr, tt.wantedOutcome, outcome)

	errs := srvBundle.shutdown()
	if len(errs) > 0 {
		t.Errorf("got %d server errors: %v", len(errs), errs)
	}
}

func toRunServerRequests(runnables []serverRunnable) []runServerRequest {
	requests := make([]runServerRequest, 0, len(runnables))
	for _, runnable := range runnables {
		requests = append(requests, runnable.toRunServerRequest())
	}
	return requests
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
