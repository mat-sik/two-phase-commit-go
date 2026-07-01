package main

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/config"
	basic "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/basic/v1"
	transfer "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/transfer/v1"
	setup "github.com/mat-sik/two-phase-commit-go/examples/internal/otel"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/adapter"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/server"
	"google.golang.org/grpc"
)

func main() {
	ctx := context.Background()

	collectorConfig, err := config.NewCollector(ctx)
	if err != nil {
		panic(err)
	}

	var participantConfig config.Participant
	participantConfig, err = config.NewParticipant(ctx)
	if err != nil {
		panic(err)
	}

	var shutdown setup.ShutdownFunc
	shutdown, err = setup.InitOTelSDK(ctx, collectorConfig.CollectorHost, collectorConfig.ServiceName)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = shutdown(ctx); err != nil {
			panic(err)
		}
	}()

	var pool *pgxpool.Pool
	if participantConfig.ShouldInitDBPool() {
		pool, err = pgxpool.New(ctx, participantConfig.DatabaseURL)
		if err != nil {
			panic(err)
		}
		defer pool.Close()
	}

	var lis net.Listener
	lis, err = newListener(participantConfig.Port)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = lis.Close(); err != nil {
			panic(err)
		}
	}()

	var srvRunner serverRunner
	var srvStopper serverStopper
	srvRunner, srvStopper, err = newServerRunner(participantConfig.Protocol, participantConfig.Mode, lis, pool)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = srvStopper(); err != nil {
			panic(err)
		}
	}()
	if err = srvRunner(lis); err != nil {
		panic(err)
	}
}

func newListener(port int) (net.Listener, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("listening on port %d: %w", port, err)
	}
	return lis, nil
}

func newServerRunner(protocol config.Protocol, mode config.Mode, lis net.Listener, pool *pgxpool.Pool) (serverRunner, serverStopper, error) {
	switch protocol {
	case config.ProtocolGRPC:
		return newGrpcServerRunner(lis, mode, pool)
	case config.ProtocolREST:
		return newRestServerRunner(mode, pool)
	default:
		panic("unsupported protocol")
	}
}

type serverRunner func(lis net.Listener) error
type serverStopper func() error

func newGrpcServerRunner(lis net.Listener, mode config.Mode, pool *pgxpool.Pool) (serverRunner, serverStopper, error) {
	srv, err := newGRPCServer(lis, mode, pool)
	if err != nil {
		return nil, nil, err
	}

	runner := func(_ net.Listener) error {
		return srv.Serve(lis)
	}

	stopper := func() error {
		srv.GracefulStop()
		return nil
	}
	return runner, stopper, nil
}

func newGRPCServer(lis net.Listener, mode config.Mode, pool *pgxpool.Pool) (*grpc.Server, error) {
	registerer := newGRPCServerRegisterer(mode, pool)
	return server.NewGRPCServer(registerer, lis.Addr()), nil
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
		panic("unsupported mode")
	}
}

func newRestServerRunner(mode config.Mode, pool *pgxpool.Pool) (serverRunner, serverStopper, error) {
	mux := newMux(mode, pool)
	srv := server.NewRESTServer(mux)

	runner := func(lis net.Listener) error {
		return srv.Serve(lis)
	}

	stopper := func() error {
		return srv.Close()
	}
	return runner, stopper, nil
}

func newMux(mode config.Mode, pool *pgxpool.Pool) *http.ServeMux {
	switch mode {
	case config.ModeTransfer:
		return adapter.NewTransferMux(pool)
	case config.ModeBasic:
		return adapter.NewBasicMux()
	default:
		panic("unsupported mode")
	}
}
