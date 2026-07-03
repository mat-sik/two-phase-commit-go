package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/config"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/basic"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/client/transfer"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/coordinator/persister"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/migrations"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/otelinit"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant"
	"github.com/mat-sik/two-phase-commit-go/twopc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	collectorConfig, err := config.NewCollector(ctx)
	if err != nil {
		slog.Error("reading collector config", "err", err)
		return 1
	}

	var coordinatorConfig config.Coordinator
	coordinatorConfig, err = config.NewCoordinator(ctx)
	if err != nil {
		slog.Error("reading coordinator config", "err", err)
		return 1
	}

	if collectorConfig.CollectorHost != "" {
		var shutdown otelinit.ShutdownFunc
		shutdown, err = otelinit.InitOTelSDK(ctx, collectorConfig.CollectorHost, collectorConfig.ServiceName)
		if err != nil {
			slog.Error("initializing OTel SDK", "err", err)
			return 1
		}
		defer func() {
			if err = shutdown(context.Background()); err != nil {
				slog.Error("shutting down OTel SDK", "err", err)
			}
		}()
	}

	var pool *pgxpool.Pool
	if coordinatorConfig.DatabaseURL != "" {
		pool, err = pgxpool.New(ctx, coordinatorConfig.DatabaseURL)
		if err != nil {
			slog.Error("creating pgx pool", "err", err)
			return 1
		}
		defer pool.Close()

		if err = migrations.Run(pool, "db/coordinator/migrations"); err != nil {
			slog.Error("running coordinator migrations", "err", err)
			return 1
		}
	}

	var lis net.Listener
	lis, err = newListener(coordinatorConfig.Port)
	if err != nil {
		slog.Error(err.Error())
		return 1
	}
	defer func() {
		if err = lis.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			slog.Error("closing listener", "err", err)
		}
	}()

	srv := &http.Server{
		Handler: newMux(pool),
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		if shutdownErr := srv.Shutdown(shutdownCtx); shutdownErr != nil {
			shutdownErr = errors.Join(shutdownErr, srv.Close())
			slog.Error("stopping server", "err", shutdownErr)
		}
	})
	defer wg.Wait()

	slog.Info("server started", "address", lis.Addr())
	if err = srv.Serve(lis); err != nil {
		cancel()
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("serving", "err", err)
			return 1
		}
	}

	slog.Info("server stopped")
	return 0
}

func newListener(port int) (net.Listener, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("listening on port %d: %w", port, err)
	}
	return lis, nil
}

func newMux(pool *pgxpool.Pool) http.Handler {
	handler := CoordinatorHandler{
		distributedTransactionUseCase: distributedTransactionUseCase{
			pool: pool,
		},
	}

	mux := http.NewServeMux()
	mux.Handle("POST /transactions", handler)

	return otelhttp.NewHandler(mux, "coordinator")
}

type CoordinatorHandler struct {
	distributedTransactionUseCase distributedTransactionUseCase
}

func (c CoordinatorHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()

	var tx distributedTransaction
	if err := json.NewDecoder(request.Body).Decode(&tx); err != nil {
		slog.ErrorContext(ctx, "decoding request body", "err", err)
		http.Error(writer, "could not read request", http.StatusInternalServerError)
		return
	}

	if err := tx.validate(); err != nil {
		slog.ErrorContext(ctx, "invalid transaction", "err", err)
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	res := c.distributedTransactionUseCase.doTx(request.Context(), tx)

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(writer).Encode(res); err != nil {
		slog.Error("failed to encode response", "err", err)
	}
}

type distributedTransactionUseCase struct {
	pool *pgxpool.Pool
}

func (c distributedTransactionUseCase) doTx(ctx context.Context, tx distributedTransaction) distributedTransactionResponse {
	clientTypeByParticipantID := newClientTypes(tx.Transactions)
	txCoordinator := c.newCoordinator(clientTypeByParticipantID)

	res := txCoordinator.Execute(ctx, tx.toTwopc())

	return newDistributedTransactionResponse(res)
}

func (c distributedTransactionUseCase) newCoordinator(clientTypeByParticipantID map[string]clientType) *twopc.Coordinator[string] {
	return twopc.NewCoordinator(c.persistenceConfig(), c.clientConfig(clientTypeByParticipantID))
}

func (c distributedTransactionUseCase) persistenceConfig() twopc.PersistenceConfig[string] {
	if c.pool == nil {
		return mockPersistenceConfig()
	}
	return postgresPersisterConfig(c.pool)
}

func mockPersistenceConfig() twopc.PersistenceConfig[string] {
	return twopc.PersistenceConfig[string]{
		TransactionStateChecker:   persister.MockTransactionStateChecker{},
		TransactionStatePersister: persister.MockTransactionStatePersister{},
	}
}

func postgresPersisterConfig(pool *pgxpool.Pool) twopc.PersistenceConfig[string] {
	return twopc.PersistenceConfig[string]{
		TransactionStateChecker:   persister.NewPostgresTransactionStateChecker(pool),
		TransactionStatePersister: persister.NewPostgresTransactionStatePersister(pool),
	}
}

func (c distributedTransactionUseCase) clientConfig(clientTypeByParticipantID map[string]clientType) twopc.ClientConfig[string] {
	newClientFunc := func(participantID string) (twopc.Client, error) {
		participantClientType, ok := clientTypeByParticipantID[participantID]
		if !ok {
			return nil, fmt.Errorf("unsupported participant: %s", participantID)
		}
		if participantClientType.protocol == config.ProtocolREST {
			return client.NewRESTClient(participantID)
		}
		if participantClientType.mode == config.ModeTransfer {
			return transfer.NewGRPCClient(participantID)
		}
		return basic.NewGRPCClient(participantID)
	}

	return twopc.ClientConfig[string]{
		NewClientFunc: newClientFunc,
	}
}

type clientType struct {
	protocol config.Protocol
	mode     config.Mode
}

func newClientTypes(transactions []transaction) map[string]clientType {
	clientTypeByParticipantID := make(map[string]clientType)
	for _, tx := range transactions {
		clientTypeByParticipantID[tx.ParticipantID] = clientType{
			protocol: tx.Protocol,
			mode:     mode(tx),
		}
	}
	return clientTypeByParticipantID
}

func mode(tx transaction) config.Mode {
	if tx.BasicPayload == nil && tx.TransferPayload != nil {
		return config.ModeTransfer
	}
	return config.ModeBasic
}

type distributedTransactionResponse struct {
	Outcome                     outcome `json:"outcome"`
	InfrastructureErrorMessages string  `json:"infrastructure_error_messages"`
}

func newDistributedTransactionResponse(res twopc.Result) distributedTransactionResponse {
	var errMessage string
	if res.Err() != nil {
		errMessage = res.Err().Error()
	}
	return distributedTransactionResponse{
		Outcome:                     newOutcome(res.Outcome()),
		InfrastructureErrorMessages: errMessage,
	}
}

type outcome string

const (
	outcomeSuccess      = "success"
	outcomeFailed       = "failed"
	outcomeInconsistent = "inconsistent"
)

func newOutcome(outcome twopc.Outcome) outcome {
	switch outcome {
	case twopc.OutcomeSuccess:
		return outcomeSuccess
	case twopc.OutcomeFailed:
		return outcomeFailed
	case twopc.OutcomeInconsistent:
		return outcomeInconsistent
	default:
		panic(fmt.Sprintf("unsupported outcome: %d", outcome))
	}
}

type distributedTransaction struct {
	ID           string        `json:"id"`
	Transactions []transaction `json:"transactions"`
}

func (dt distributedTransaction) validate() error {
	var errs []error
	for _, tx := range dt.Transactions {
		if err := tx.validate(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (dt distributedTransaction) toTwopc() twopc.DistributedTransaction[string] {
	transactions := make([]twopc.Transaction[string], 0, len(dt.Transactions))
	for _, tx := range dt.Transactions {
		transactions = append(transactions, tx.toTwopc())
	}
	return twopc.DistributedTransaction[string]{
		TransactionID: dt.ID,
		Transactions:  transactions,
	}
}

type transaction struct {
	ParticipantID   string                       `json:"participant_id"`
	BasicPayload    *BasicPayload                `json:"basic_payload"`
	TransferPayload *participant.TransferPayload `json:"transfer_payload"`
	Protocol        config.Protocol              `json:"protocol"`
}

func (tx transaction) validate() error {
	if tx.ParticipantID == "" {
		return errors.New("participant id should have non zero value")
	}
	if tx.BasicPayload == nil && tx.TransferPayload == nil {
		return errors.New("basic_payload and transfer_payload should not be both nil")
	}
	if tx.Protocol != config.ProtocolGRPC && tx.Protocol != config.ProtocolREST {
		return fmt.Errorf("unsupported protocol %s", tx.Protocol)
	}
	return nil
}

func (tx transaction) toTwopc() twopc.Transaction[string] {
	var payload twopc.PreparePayload
	if tx.BasicPayload != nil {
		payload = *tx.BasicPayload
	} else if tx.TransferPayload != nil {
		payload = *tx.TransferPayload
	} else {
		panic("either basic of transfer payload should be non nil")
	}

	return twopc.Transaction[string]{
		ParticipantID: tx.ParticipantID,
		Payload:       payload,
	}
}

type BasicPayload struct {
	Payload string `json:"payload"`
}
