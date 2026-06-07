package adapter

import (
	"context"

	basic "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/basic/v1"
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
