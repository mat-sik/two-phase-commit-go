package adapter

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/transfer/v1"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewTransferGRPCHandler(pool *pgxpool.Pool) *GRPCTransferHandler {
	handler := participant.NewTransferTransactionHandler(pool)
	return &GRPCTransferHandler{
		transactionPreparer:   handler,
		transactionCommiter:   handler,
		transactionRollbacker: handler,
	}
}

type GRPCTransferHandler struct {
	pb.UnimplementedTransferServiceServer

	transactionPreparer   TransferTransactionPreparer
	transactionCommiter   TransactionCommiter
	transactionRollbacker TransactionRollbacker
}

func (h *GRPCTransferHandler) PrepareTransaction(ctx context.Context, req *pb.PrepareTransactionRequest) (*pb.PrepareTransactionResponse, error) {
	payload := req.GetPayload()
	transferPayload := participant.TransferPayload{
		SenderID:   payload.GetSenderId(),
		ReceiverID: payload.GetReceiverId(),
		Amount:     payload.GetAmount(),
	}
	transactionID := req.GetTransactionId()
	if err := h.transactionPreparer.PrepareTransaction(ctx, transactionID, transferPayload); err != nil {
		slog.ErrorContext(ctx, "preparing tx", "transactionID", transactionID, "err", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.PrepareTransactionResponse{}, nil
}

func (h *GRPCTransferHandler) CommitTransaction(ctx context.Context, req *pb.CommitTransactionRequest) (*pb.CommitTransactionResponse, error) {
	transactionID := req.GetTransactionId()
	if err := h.transactionCommiter.CommitTransaction(ctx, transactionID); err != nil {
		slog.ErrorContext(ctx, "committing tx", "transactionID", transactionID, "err", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.CommitTransactionResponse{}, nil
}

func (h *GRPCTransferHandler) RollbackTransaction(ctx context.Context, req *pb.RollbackTransactionRequest) (*pb.RollbackTransactionResponse, error) {
	transactionID := req.GetTransactionId()
	if err := h.transactionRollbacker.RollbackTransaction(ctx, transactionID); err != nil {
		slog.ErrorContext(ctx, "rolling back tx", "transactionID", transactionID, "err", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.RollbackTransactionResponse{}, nil
}
