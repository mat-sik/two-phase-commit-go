package main

import (
	"context"
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
	basic "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/basic/v1"
	transfer "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/transfer/v1"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/migrations"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/otelinit"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/adapter"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/server"
	"google.golang.org/grpc"
)

// TODO: inspect why shut down takes so long
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

	var participantConfig config.Participant
	participantConfig, err = config.NewParticipant(ctx)
	if err != nil {
		slog.Error("reading participant config", "err", err)
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
	if participantConfig.ShouldInitDBPool() {
		pool, err = pgxpool.New(ctx, participantConfig.DatabaseURL)
		if err != nil {
			slog.Error("creating pgx pool", "err", err)
			return 1
		}
		defer pool.Close()

		if err = migrations.Run(pool, "db/participant/transfer/migrations"); err != nil {
			slog.Error("running transfer participant migrations", "err", err)
			return 1
		}
	}

	var lis net.Listener
	lis, err = newListener(participantConfig.Port)
	if err != nil {
		slog.Error(err.Error())
		return 1
	}
	defer func() {
		if err = lis.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			slog.Error("closing listener", "err", err)
		}
	}()

	srvRunner, srvStopper := newServerHandles(participantConfig.Protocol, participantConfig.Mode, lis, pool)

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() {
		<-ctx.Done()
		if stopErr := srvStopper(); stopErr != nil {
			slog.Error("stopping server", "err", stopErr)
		}
	})
	defer wg.Wait()

	slog.Info("server started", "address", lis.Addr())
	if err = srvRunner(lis); err != nil {
		cancel()
		if !isGracefulShutdown(err) {
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

func newServerHandles(protocol config.Protocol, mode config.Mode, lis net.Listener, pool *pgxpool.Pool) (serverRunner, serverStopper) {
	switch protocol {
	case config.ProtocolGRPC:
		return newGrpcServerHandles(lis, mode, pool)
	case config.ProtocolREST:
		return newRestServerHandles(mode, pool)
	default:
		panic(fmt.Sprintf("unsupported protocol: %s", protocol))
	}
}

type serverRunner func(lis net.Listener) error
type serverStopper func() error

func newGrpcServerHandles(lis net.Listener, mode config.Mode, pool *pgxpool.Pool) (serverRunner, serverStopper) {
	srv := newGRPCServer(lis, mode, pool)

	runner := func(_ net.Listener) error {
		if err := srv.Serve(lis); err != nil {
			return fmt.Errorf("serving gRPC server: %w", err)
		}
		return nil
	}

	stopper := func() error {
		srv.GracefulStop()
		return nil
	}

	return runner, stopper
}

func newGRPCServer(lis net.Listener, mode config.Mode, pool *pgxpool.Pool) *grpc.Server {
	registerer := newGRPCServerRegisterer(mode, pool)
	return server.NewGRPCServer(registerer, lis.Addr())
}

func newGRPCServerRegisterer(mode config.Mode, pool *pgxpool.Pool) server.GRPCHandlerRegisterer {
	switch mode {
	case config.ModeTransfer:
		handler := adapter.NewTransferGRPCHandler(pool)
		return func(server *grpc.Server) {
			transfer.RegisterTransferServiceServer(server, handler)
		}
	case config.ModeBasic:
		handler := adapter.NewBasicGRPCHandler()
		return func(server *grpc.Server) {
			basic.RegisterBasicServiceServer(server, handler)
		}
	default:
		panic(fmt.Sprintf("unsupported mode: %s", mode))
	}
}

func newRestServerHandles(mode config.Mode, pool *pgxpool.Pool) (serverRunner, serverStopper) {
	mux := newMux(mode, pool)
	srv := server.NewRESTServer(mux)

	runner := func(lis net.Listener) error {
		if err := srv.Serve(lis); err != nil {
			return fmt.Errorf("serving REST server: %w", err)
		}
		return nil
	}

	stopper := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			err = errors.Join(err, srv.Close())
			return fmt.Errorf("closing REST server: %w", err)
		}
		return nil
	}

	return runner, stopper
}

func newMux(mode config.Mode, pool *pgxpool.Pool) *http.ServeMux {
	switch mode {
	case config.ModeTransfer:
		return adapter.NewTransferMux(pool)
	case config.ModeBasic:
		return adapter.NewBasicMux()
	default:
		panic(fmt.Sprintf("unsupported mode: %s", mode))
	}
}

func isGracefulShutdown(err error) bool {
	return errors.Is(err, http.ErrServerClosed) || errors.Is(err, grpc.ErrServerStopped)
}
