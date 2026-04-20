package main

import (
	"log/slog"
	"net"

	"github.com/mat-sik/two-phase-commit-go/grpc-unary/internal/client"
)

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		slog.Error("failed to listen:", err)
		return
	}

	if err = client.RunServer(listener, client.NewNoopHandler()); err != nil {
		slog.Error("server failed:", err)
	}
}
