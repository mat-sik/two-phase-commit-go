package twopc

import (
	"context"
	"fmt"

	pb "github.com/mat-sik/two-phase-commit-go/grpc-unary/internal/generated/client/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type grpcClient struct {
	client pb.ClientServiceClient
}

func newGRPCClient(identifiable client) (Client, error) {
	conn, err := grpc.NewClient(string(identifiable.ClientIdentifier()), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create client for %s: %w", identifiable.ClientIdentifier(), err)
	}
	client := pb.NewClientServiceClient(conn)
	return grpcClient{client: client}, nil
}

func (c grpcClient) prepareTransaction(ctx context.Context, transactionID string, operation prepareOperation) error {
	req := pb.PrepareTransactionRequest{TransactionId: transactionID, Payload: operation.payload}
	_, err := c.client.PrepareTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}

func (c grpcClient) commitTransaction(ctx context.Context, transactionID string) error {
	req := pb.CommitTransactionRequest{TransactionId: transactionID}
	_, err := c.client.CommitTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}

func (c grpcClient) rollbackTransaction(ctx context.Context, transactionID string) error {
	req := pb.RollbackTransactionRequest{TransactionId: transactionID}
	_, err := c.client.RollbackTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}
