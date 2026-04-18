package coordinator

import (
	"fmt"
	"sync"

	pb "github.com/mat-sik/two-phase-commit-go/grpc-unary/internal/generated/client/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type clientRegistrar struct {
	store *clientRegistrarStore
}

func (cr *clientRegistrar) getClient(gRPCHost string) (pb.ClientServiceClient, error) {
	client, ok := cr.store.load(gRPCHost)
	if !ok {
		conn, err := grpc.NewClient(gRPCHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to create client for %s: %w", gRPCHost, err)
		}
		client = pb.NewClientServiceClient(conn)
		cr.store.add(gRPCHost, client)
	}
	return client, nil
}

type clientRegistrarStore struct {
	store sync.Map
}

func (store *clientRegistrarStore) add(gRPCHost string, client pb.ClientServiceClient) {
	store.store.Store(gRPCHost, client)
}

func (store *clientRegistrarStore) load(gRPCHost string) (pb.ClientServiceClient, bool) {
	value, ok := store.store.Load(gRPCHost)
	if !ok {
		return nil, false
	}
	return value.(pb.ClientServiceClient), true
}
