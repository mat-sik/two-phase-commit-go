package coordinator

import (
	"context"
	"fmt"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
	basic "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/basic/v1"
	transfer "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/transfer/v1"
	"github.com/mat-sik/two-phase-commit-go/twopc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type basicGRPCClient struct {
	client basic.BasicServiceClient
}

func newBasicGRPCClient(clientID string) (twopc.Client, error) {
	conn, err := grpc.NewClient(clientID, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create client for %s: %w", clientID, err)
	}
	return basicGRPCClient{
		client: basic.NewBasicServiceClient(conn),
	}, nil
}

func (c basicGRPCClient) PrepareTransaction(ctx context.Context, transactionID string, payload twopc.PreparePayload) error {
	req := basic.PrepareTransactionRequest{TransactionId: transactionID, Payload: payload.(string)}
	_, err := c.client.PrepareTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}

func (c basicGRPCClient) CommitTransaction(ctx context.Context, transactionID string) error {
	req := basic.CommitTransactionRequest{TransactionId: transactionID}
	_, err := c.client.CommitTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}

func (c basicGRPCClient) RollbackTransaction(ctx context.Context, transactionID string) error {
	req := basic.RollbackTransactionRequest{TransactionId: transactionID}
	_, err := c.client.RollbackTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}

type transferGRPCClient struct {
	client transfer.TransferServiceClient
}

func newTransferGRPCClient(clientID string) (twopc.Client, error) {
	conn, err := grpc.NewClient(clientID, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create client for %s: %w", clientID, err)
	}
	return transferGRPCClient{
		client: transfer.NewTransferServiceClient(conn),
	}, nil
}

func (c transferGRPCClient) PrepareTransaction(ctx context.Context, transactionID string, payload twopc.PreparePayload) error {
	const method = "PrepareTransaction"

	transferPayload, ok := payload.(client.TransferPayload)
	if !ok {
		panic(fmt.Sprintf("%s: unexpected payload type %T", method, payload))
	}

	grpcTransferPayload := &transfer.TransferPayload{
		SenderId:   transferPayload.SenderID,
		ReceiverId: transferPayload.ReceiverID,
		Amount:     transferPayload.Amount,
	}

	req := transfer.PrepareTransactionRequest{TransactionId: transactionID, Payload: grpcTransferPayload}
	_, err := c.client.PrepareTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}

func (c transferGRPCClient) CommitTransaction(ctx context.Context, transactionID string) error {
	req := transfer.CommitTransactionRequest{TransactionId: transactionID}
	_, err := c.client.CommitTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}

func (c transferGRPCClient) RollbackTransaction(ctx context.Context, transactionID string) error {
	req := transfer.RollbackTransactionRequest{TransactionId: transactionID}
	_, err := c.client.RollbackTransaction(ctx, &req, grpc.WaitForReady(true))
	return err
}
