package client

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/transfer/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewSQLGRPCHandler(pool *pgxpool.Pool) *GRPCTransferHandler {
	handler := &sqlTransactionHandler{
		pool: pool,
	}
	return &GRPCTransferHandler{
		transactionPreparer:   handler,
		transactionCommiter:   handler,
		transactionRollbacker: handler,
	}
}

type GRPCTransferHandler struct {
	pb.UnimplementedTransferServiceServer

	transactionPreparer   transactionPreparer
	transactionCommiter   transactionCommiter
	transactionRollbacker transactionRollbacker
}

func (h *GRPCTransferHandler) PrepareTransaction(ctx context.Context, req *pb.PrepareTransactionRequest) (*pb.PrepareTransactionResponse, error) {
	payload := req.GetPayload()
	transferPayload := TransferPayload{
		SenderID:   payload.GetSenderId(),
		ReceiverID: payload.GetReceiverId(),
		Amount:     payload.GetAmount(),
	}
	err := h.transactionPreparer.prepareTransaction(ctx, req.GetTransactionId(), transferPayload)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.PrepareTransactionResponse{}, nil
}

func (h *GRPCTransferHandler) CommitTransaction(ctx context.Context, req *pb.CommitTransactionRequest) (*pb.CommitTransactionResponse, error) {
	err := h.transactionCommiter.commitTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.CommitTransactionResponse{}, nil
}

func (h *GRPCTransferHandler) RollbackTransaction(ctx context.Context, req *pb.RollbackTransactionRequest) (*pb.RollbackTransactionResponse, error) {
	err := h.transactionRollbacker.rollbackTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.RollbackTransactionResponse{}, nil
}
