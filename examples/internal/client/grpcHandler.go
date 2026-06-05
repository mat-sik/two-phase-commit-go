package client

import (
	"context"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/basic/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewSQLGRPCHandler(pool *pgxpool.Pool) *GRPCHandler {
	handler := &sqlTransactionHandler{
		pool: pool,
	}
	return &GRPCHandler{
		transactionPreparer:   handler,
		transactionCommiter:   handler,
		transactionRollbacker: handler,
	}
}

func NewNoopGRPCHandler() *GRPCHandler {
	handler := &noopTransactionHandler{
		transactionStatusMap:     &transactionStatusMap{},
		prepareFailUntilAttempt:  atomic.Int64{},
		commitFailUntilAttempt:   atomic.Int64{},
		rollbackFailUntilAttempt: atomic.Int64{},
	}
	handler.prepareFailUntilAttempt.Store(0)
	handler.commitFailUntilAttempt.Store(0)
	handler.rollbackFailUntilAttempt.Store(0)
	return &GRPCHandler{
		transactionPreparer:   handler,
		transactionCommiter:   handler,
		transactionRollbacker: handler,
	}
}

func NewFailingNoopGRPCHandler(prepareFailUntilAttempt, commitFailUntilAttempt, rollbackFailUntilAttempt int) *GRPCHandler {
	handler := &noopTransactionHandler{
		transactionStatusMap:     &transactionStatusMap{},
		prepareFailUntilAttempt:  atomic.Int64{},
		commitFailUntilAttempt:   atomic.Int64{},
		rollbackFailUntilAttempt: atomic.Int64{},
	}
	handler.prepareFailUntilAttempt.Store(int64(prepareFailUntilAttempt))
	handler.commitFailUntilAttempt.Store(int64(commitFailUntilAttempt))
	handler.rollbackFailUntilAttempt.Store(int64(rollbackFailUntilAttempt))
	return &GRPCHandler{
		transactionPreparer:   handler,
		transactionCommiter:   handler,
		transactionRollbacker: handler,
	}
}

type GRPCHandler struct {
	pb.UnimplementedBasicServiceServer

	transactionPreparer   transactionPreparer
	transactionCommiter   transactionCommiter
	transactionRollbacker transactionRollbacker
}

func (h *GRPCHandler) PrepareTransaction(ctx context.Context, req *pb.PrepareTransactionRequest) (*pb.PrepareTransactionResponse, error) {
	err := h.transactionPreparer.prepareTransaction(ctx, req.GetTransactionId(), req.GetPayload())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.PrepareTransactionResponse{}, nil
}

func (h *GRPCHandler) CommitTransaction(ctx context.Context, req *pb.CommitTransactionRequest) (*pb.CommitTransactionResponse, error) {
	err := h.transactionCommiter.commitTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.CommitTransactionResponse{}, nil
}

func (h *GRPCHandler) RollbackTransaction(ctx context.Context, req *pb.RollbackTransactionRequest) (*pb.RollbackTransactionResponse, error) {
	err := h.transactionRollbacker.rollbackTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.RollbackTransactionResponse{}, nil
}
