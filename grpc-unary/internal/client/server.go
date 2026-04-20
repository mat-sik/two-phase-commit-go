package client

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "github.com/mat-sik/two-phase-commit-go/grpc-unary/internal/generated/client/v1"
	"google.golang.org/grpc"
)

func RunServer(listener net.Listener, handler *Handler) error {
	s := grpc.NewServer()
	pb.RegisterClientServiceServer(s, handler)

	var wg sync.WaitGroup
	wg.Add(1)
	go stopServerOnInterrupt(&wg, s)

	slog.Info("ClientService gRPC server listening", "addr", listener.Addr().String())
	if err := s.Serve(listener); err != nil {
		return err
	}
	wg.Wait()
	return nil
}

func stopServerOnInterrupt(wg *sync.WaitGroup, s *grpc.Server) {
	defer wg.Done()
	blockUntilSignal()
	stopServer(s)
}

func blockUntilSignal() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}

func stopServer(s *grpc.Server) {
	slog.Info("shutting down gRPC server...")

	var wg sync.WaitGroup
	wg.Add(1)

	var mutex sync.Mutex
	stoppingStatus := notStopped

	stopWaiting := make(chan struct{}, 1)
	go func() {
		defer wg.Done()

		s.GracefulStop()

		mutex.Lock()
		if stoppingStatus == notStopped {
			stoppingStatus = stoppedGracefully
			slog.Info("Stopped gRPC server gracefully")
		}
		mutex.Unlock()

		stopWaiting <- struct{}{}
	}()

	select {
	case <-time.After(10 * time.Second):
	case <-stopWaiting:
	}

	mutex.Lock()
	if stoppingStatus == notStopped {
		s.Stop()
		stoppingStatus = stoppedForcefully
		slog.Warn("Stopped gRPC server forcefully")
	}
	mutex.Unlock()

	wg.Wait()
}

type stopStatus int

const (
	notStopped stopStatus = iota
	stoppedGracefully
	stoppedForcefully
)
