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

func NewGRPCClient(participantID string) (twopc.Client, error) {
	conn, err := grpc.NewClient(participantID, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("creating gRPC conn %s: %w", participantID, err)
	}
	return basicGRPCClient{
		client: pb.NewBasicServiceClient(conn),
	}, nil
}

func (c basicGRPCClient) PrepareTransaction(ctx context.Context, transactionID string, payload twopc.PreparePayload) error {
	req := pb.PrepareTransactionRequest{TransactionId: transactionID, Payload: payload.(string)}
	if _, err := c.client.PrepareTransaction(ctx, &req, grpc.WaitForReady(true)); err != nil {
		return fmt.Errorf("gRPC sending prepare tx %s payload %v: %w", transactionID, payload, err)
	}
	return nil
}

func (c basicGRPCClient) CommitTransaction(ctx context.Context, transactionID string) error {
	req := pb.CommitTransactionRequest{TransactionId: transactionID}
	if _, err := c.client.CommitTransaction(ctx, &req, grpc.WaitForReady(true)); err != nil {
		return fmt.Errorf("gRPC sending commit tx %s: %w", transactionID, err)
	}
	return nil
}

func (c basicGRPCClient) RollbackTransaction(ctx context.Context, transactionID string) error {
	req := pb.RollbackTransactionRequest{TransactionId: transactionID}
	if _, err := c.client.RollbackTransaction(ctx, &req, grpc.WaitForReady(true)); err != nil {
		return fmt.Errorf("gRPC sending rollback tx %s: %w", transactionID, err)
	}
	return nil
}
