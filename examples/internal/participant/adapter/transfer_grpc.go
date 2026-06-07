package adapter

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/transfer/v1"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// todo: do the same for rest
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
	err := h.transactionPreparer.PrepareTransaction(ctx, req.GetTransactionId(), transferPayload)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.PrepareTransactionResponse{}, nil
}

func (h *GRPCTransferHandler) CommitTransaction(ctx context.Context, req *pb.CommitTransactionRequest) (*pb.CommitTransactionResponse, error) {
	err := h.transactionCommiter.CommitTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.CommitTransactionResponse{}, nil
}

func (h *GRPCTransferHandler) RollbackTransaction(ctx context.Context, req *pb.RollbackTransactionRequest) (*pb.RollbackTransactionResponse, error) {
	err := h.transactionRollbacker.RollbackTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.RollbackTransactionResponse{}, nil
}
