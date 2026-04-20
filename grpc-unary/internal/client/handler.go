package client

import (
	"context"
	"log/slog"

	pb "github.com/mat-sik/two-phase-commit-go/grpc-unary/internal/generated/client/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	pb.UnimplementedClientServiceServer

	transactionPreparer   transactionPreparer
	transactionCommiter   transactionCommiter
	transactionRollbacker transactionRollbacker
}

func (h *Handler) PrepareTransaction(ctx context.Context, req *pb.PrepareTransactionRequest) (*pb.PrepareTransactionResponse, error) {
	slog.Info("PrepareTransaction called", "request", req)
	ok, err := h.transactionPreparer.prepareTransaction(ctx, req.GetTransactionId(), req.GetPayload())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !ok {
		return nil, status.Error(codes.Aborted, "prepare transaction failed")
	}
	return &pb.PrepareTransactionResponse{}, nil
}

func (h *Handler) CommitTransaction(ctx context.Context, req *pb.CommitTransactionRequest) (*pb.CommitTransactionResponse, error) {
	slog.Info("CommitTransaction called", "request", req)
	ok, err := h.transactionCommiter.commitTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !ok {
		return nil, status.Error(codes.Aborted, "commit transaction failed")
	}
	return &pb.CommitTransactionResponse{}, nil
}

func (h *Handler) RollbackTransaction(ctx context.Context, req *pb.RollbackTransactionRequest) (*pb.RollbackTransactionResponse, error) {
	slog.Info("RollbackTransaction called", "request", req)
	ok, err := h.transactionRollbacker.rollbackTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !ok {
		return nil, status.Error(codes.Aborted, "rollback transaction failed")
	}
	return &pb.RollbackTransactionResponse{}, nil
}
