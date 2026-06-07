package client

import (
	"context"
	"net"

	"google.golang.org/grpc"
)

func NewGRPCServer(registerer GRPCHandlerRegisterer, addr net.Addr) *grpc.Server {
	server := grpc.NewServer(
		grpc.UnaryInterceptor(serverAddrInterceptor(addr)),
	)
	registerer(server)
	return server
}

type GRPCHandlerRegisterer func(server *grpc.Server)

func serverAddrInterceptor(addr net.Addr) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(contextWithAddress(ctx, addr), req)
	}
}
