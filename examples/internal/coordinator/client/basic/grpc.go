package basic

import (
	"context"
	"fmt"

	pb "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/basic/v1"
	"github.com/mat-sik/two-phase-commit-go/twopc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type basicGRPCClient struct {
	client pb.BasicServiceClient
}

func NewGRPCClient(clientID string) (twopc.Client, error) {
	conn, err := grpc.NewClient(clientID, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create client for %s: %w", clientID, err)
	}
	return basicGRPCClient{
		client: pb.NewBasicServiceClient(conn),
	}, nil
}

func (c basicGRPCClient) PrepareTransaction(ctx context.Context, transactionID string, payload twopc.PreparePayload) error {
	req := pb.PrepareTransactionRequest{TransactionId: transactionID, Payload: payload.(string)}
	_, err := c.client.PrepareTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}

func (c basicGRPCClient) CommitTransaction(ctx context.Context, transactionID string) error {
	req := pb.CommitTransactionRequest{TransactionId: transactionID}
	_, err := c.client.CommitTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}

func (c basicGRPCClient) RollbackTransaction(ctx context.Context, transactionID string) error {
	req := pb.RollbackTransactionRequest{TransactionId: transactionID}
	_, err := c.client.RollbackTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}
