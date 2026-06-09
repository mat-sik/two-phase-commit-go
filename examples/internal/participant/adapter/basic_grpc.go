package adapter

import (
	"context"
	"log/slog"

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

	transactionPreparer BasicTransactionPreparer
	// todo: rename to committer
	transactionCommiter   TransactionCommiter
	transactionRollbacker TransactionRollbacker
}

func (h *GRPCBasicHandler) PrepareTransaction(ctx context.Context, req *basic.PrepareTransactionRequest) (*basic.PrepareTransactionResponse, error) {
	transactionID := req.GetTransactionId()
	if err := h.transactionPreparer.PrepareTransaction(ctx, transactionID, req.GetPayload()); err != nil {
		slog.ErrorContext(ctx, "preparing tx", "transactionID", transactionID, "err", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &basic.PrepareTransactionResponse{}, nil
}

func (h *GRPCBasicHandler) CommitTransaction(ctx context.Context, req *basic.CommitTransactionRequest) (*basic.CommitTransactionResponse, error) {
	transactionID := req.GetTransactionId()
	if err := h.transactionCommiter.CommitTransaction(ctx, transactionID); err != nil {
		slog.ErrorContext(ctx, "committing tx", "transactionID", transactionID, "err", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &basic.CommitTransactionResponse{}, nil
}

func (h *GRPCBasicHandler) RollbackTransaction(ctx context.Context, req *basic.RollbackTransactionRequest) (*basic.RollbackTransactionResponse, error) {
	transactionID := req.GetTransactionId()
	if err := h.transactionRollbacker.RollbackTransaction(ctx, transactionID); err != nil {
		slog.ErrorContext(ctx, "rolling back tx", "transactionID", transactionID, "err", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &basic.RollbackTransactionResponse{}, nil
}
