package coordinator

import (
	"context"
	"fmt"

	pb "github.com/mat-sik/two-phase-commit-go/grpc-unary/internal/generated/client/v1"
	"github.com/mat-sik/two-phase-commit-go/twopc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type grpcClient struct {
	client pb.ClientServiceClient
}

func NewGRPCClient(clientID string) (twopc.Client, error) {
	conn, err := grpc.NewClient(clientID, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create client for %s: %w", clientID, err)
	}
	client := pb.NewClientServiceClient(conn)
	return grpcClient{client: client}, nil
}

func (c grpcClient) PrepareTransaction(ctx context.Context, transactionID string, payload twopc.PreparePayload) error {
	req := pb.PrepareTransactionRequest{TransactionId: transactionID, Payload: payload.(string)}
	_, err := c.client.PrepareTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}

func (c grpcClient) CommitTransaction(ctx context.Context, transactionID string) error {
	req := pb.CommitTransactionRequest{TransactionId: transactionID}
	_, err := c.client.CommitTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}

func (c grpcClient) RollbackTransaction(ctx context.Context, transactionID string) error {
	req := pb.RollbackTransactionRequest{TransactionId: transactionID}
	_, err := c.client.RollbackTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}
