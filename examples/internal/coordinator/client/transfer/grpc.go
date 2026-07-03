package transfer

import (
	"context"
	"fmt"

	pb "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/transfer/v1"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant"
	"github.com/mat-sik/two-phase-commit-go/twopc"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type transferGRPCClient struct {
	client pb.TransferServiceClient
}

func NewGRPCClient(participantID string) (twopc.Client, error) {
	conn, err := grpc.NewClient(
		participantID,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("creating gRPC conn %s: %w", participantID, err)
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
	if _, err := c.client.PrepareTransaction(ctx, &req, grpc.WaitForReady(true)); err != nil {
		err = fmt.Errorf("gRPC sending prepare tx %s payload %v: %w", transactionID, payload, err)
	}
	return nil
}

func (c transferGRPCClient) CommitTransaction(ctx context.Context, transactionID string) error {
	req := pb.CommitTransactionRequest{TransactionId: transactionID}
	if _, err := c.client.CommitTransaction(ctx, &req, grpc.WaitForReady(true)); err != nil {
		err = fmt.Errorf("gRPC sending commit tx %s: %w", transactionID, err)
	}
	return nil
}

func (c transferGRPCClient) RollbackTransaction(ctx context.Context, transactionID string) error {
	req := pb.RollbackTransactionRequest{TransactionId: transactionID}
	if _, err := c.client.RollbackTransaction(ctx, &req, grpc.WaitForReady(true)); err != nil {
		err = fmt.Errorf("gRPC sending rollback tx %s: %w", transactionID, err)
	}
	return nil
}
