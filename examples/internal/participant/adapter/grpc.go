package adapter

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	basic "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/basic/v1"
	transfer "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/transfer/v1"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewBasicGRPCHandler() *GRPCBasicHandler {
	handler := participant.NewBasicTransactionHandler()
	return &GRPCBasicHandler{
		transactionPreparer:   handler,
		transactionCommiter:   handler,
		transactionRollbacker: handler,
	}
}

func NewFailingBasicGRPCHandler(prepareFailUntilAttempt, commitFailUntilAttempt, rollbackFailUntilAttempt int) *GRPCBasicHandler {
	handler := participant.NewFailingBasicTransactionHandler(prepareFailUntilAttempt, commitFailUntilAttempt, rollbackFailUntilAttempt)
	return &GRPCBasicHandler{
		transactionPreparer:   handler,
		transactionCommiter:   handler,
		transactionRollbacker: handler,
	}
}

type GRPCBasicHandler struct {
	basic.UnimplementedBasicServiceServer

	transactionPreparer   BasicTransactionPreparer
	transactionCommiter   TransactionCommiter
	transactionRollbacker TransactionRollbacker
}

func (h *GRPCBasicHandler) PrepareTransaction(ctx context.Context, req *basic.PrepareTransactionRequest) (*basic.PrepareTransactionResponse, error) {
	err := h.transactionPreparer.PrepareTransaction(ctx, req.GetTransactionId(), req.GetPayload())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &basic.PrepareTransactionResponse{}, nil
}

func (h *GRPCBasicHandler) CommitTransaction(ctx context.Context, req *basic.CommitTransactionRequest) (*basic.CommitTransactionResponse, error) {
	err := h.transactionCommiter.CommitTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &basic.CommitTransactionResponse{}, nil
}

func (h *GRPCBasicHandler) RollbackTransaction(ctx context.Context, req *basic.RollbackTransactionRequest) (*basic.RollbackTransactionResponse, error) {
	err := h.transactionRollbacker.RollbackTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &basic.RollbackTransactionResponse{}, nil
}

func NewTransferGRPCHandler(pool *pgxpool.Pool) *GRPCTransferHandler {
	handler := participant.NewTransferTransactionHandler(pool)
	return &GRPCTransferHandler{
		transactionPreparer:   handler,
		transactionCommiter:   handler,
		transactionRollbacker: handler,
	}
}

type GRPCTransferHandler struct {
	transfer.UnimplementedTransferServiceServer

	transactionPreparer   TransferTransactionPreparer
	transactionCommiter   TransactionCommiter
	transactionRollbacker TransactionRollbacker
}

func (h *GRPCTransferHandler) PrepareTransaction(ctx context.Context, req *transfer.PrepareTransactionRequest) (*transfer.PrepareTransactionResponse, error) {
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
	return &transfer.PrepareTransactionResponse{}, nil
}

func (h *GRPCTransferHandler) CommitTransaction(ctx context.Context, req *transfer.CommitTransactionRequest) (*transfer.CommitTransactionResponse, error) {
	err := h.transactionCommiter.CommitTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &transfer.CommitTransactionResponse{}, nil
}

func (h *GRPCTransferHandler) RollbackTransaction(ctx context.Context, req *transfer.RollbackTransactionRequest) (*transfer.RollbackTransactionResponse, error) {
	err := h.transactionRollbacker.RollbackTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &transfer.RollbackTransactionResponse{}, nil
}
