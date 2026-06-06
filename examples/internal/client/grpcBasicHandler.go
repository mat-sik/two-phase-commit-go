package client

import (
	"context"
	"sync/atomic"

	pb "github.com/mat-sik/two-phase-commit-go/examples/internal/generated/basic/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewNoopGRPCHandler() *GRPCBasicHandler {
	handler := &noopTransactionHandler{
		transactionStatusMap:     &transactionStatusMap{},
		prepareFailUntilAttempt:  atomic.Int64{},
		commitFailUntilAttempt:   atomic.Int64{},
		rollbackFailUntilAttempt: atomic.Int64{},
	}
	handler.prepareFailUntilAttempt.Store(0)
	handler.commitFailUntilAttempt.Store(0)
	handler.rollbackFailUntilAttempt.Store(0)
	return &GRPCBasicHandler{
		transactionPreparer:   handler,
		transactionCommiter:   handler,
		transactionRollbacker: handler,
	}
}

func NewFailingNoopGRPCHandler(prepareFailUntilAttempt, commitFailUntilAttempt, rollbackFailUntilAttempt int) *GRPCBasicHandler {
	handler := &noopTransactionHandler{
		transactionStatusMap:     &transactionStatusMap{},
		prepareFailUntilAttempt:  atomic.Int64{},
		commitFailUntilAttempt:   atomic.Int64{},
		rollbackFailUntilAttempt: atomic.Int64{},
	}
	handler.prepareFailUntilAttempt.Store(int64(prepareFailUntilAttempt))
	handler.commitFailUntilAttempt.Store(int64(commitFailUntilAttempt))
	handler.rollbackFailUntilAttempt.Store(int64(rollbackFailUntilAttempt))
	return &GRPCBasicHandler{
		transactionPreparer:   handler,
		transactionCommiter:   handler,
		transactionRollbacker: handler,
	}
}

type GRPCBasicHandler struct {
	pb.UnimplementedBasicServiceServer

	transactionPreparer   transactionPreparer
	transactionCommiter   transactionCommiter
	transactionRollbacker transactionRollbacker
}

func (h *GRPCBasicHandler) PrepareTransaction(ctx context.Context, req *pb.PrepareTransactionRequest) (*pb.PrepareTransactionResponse, error) {
	err := h.transactionPreparer.prepareTransaction(ctx, req.GetTransactionId(), req.GetPayload())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.PrepareTransactionResponse{}, nil
}

func (h *GRPCBasicHandler) CommitTransaction(ctx context.Context, req *pb.CommitTransactionRequest) (*pb.CommitTransactionResponse, error) {
	err := h.transactionCommiter.commitTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.CommitTransactionResponse{}, nil
}

func (h *GRPCBasicHandler) RollbackTransaction(ctx context.Context, req *pb.RollbackTransactionRequest) (*pb.RollbackTransactionResponse, error) {
	err := h.transactionRollbacker.rollbackTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.RollbackTransactionResponse{}, nil
}
