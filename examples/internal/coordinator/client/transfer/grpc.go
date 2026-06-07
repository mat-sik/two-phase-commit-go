package transfer

import (
	"context"
	"fmt"

	pb "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/transfer/v1"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant"
	"github.com/mat-sik/two-phase-commit-go/twopc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type transferGRPCClient struct {
	client pb.TransferServiceClient
}

func NewGRPCClient(clientID string) (twopc.Client, error) {
	conn, err := grpc.NewClient(clientID, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create client for %s: %w", clientID, err)
	}
	return transferGRPCClient{
		client: pb.NewTransferServiceClient(conn),
	}, nil
}

func (c transferGRPCClient) PrepareTransaction(ctx context.Context, transactionID string, payload twopc.PreparePayload) error {
	const method = "PrepareTransaction"

	transferPayload, ok := payload.(participant.TransferPayload)
	if !ok {
		panic(fmt.Sprintf("%s: unexpected payload type %T", method, payload))
	}

	grpcTransferPayload := &pb.TransferPayload{
		SenderId:   transferPayload.SenderID,
		ReceiverId: transferPayload.ReceiverID,
		Amount:     transferPayload.Amount,
	}

	req := pb.PrepareTransactionRequest{TransactionId: transactionID, Payload: grpcTransferPayload}
	_, err := c.client.PrepareTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}

func (c transferGRPCClient) CommitTransaction(ctx context.Context, transactionID string) error {
	req := pb.CommitTransactionRequest{TransactionId: transactionID}
	_, err := c.client.CommitTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}

func (c transferGRPCClient) RollbackTransaction(ctx context.Context, transactionID string) error {
	req := pb.RollbackTransactionRequest{TransactionId: transactionID}
	_, err := c.client.RollbackTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}
