package client

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type noopTransactionHandler struct {
	transactionStatusMap     *transactionStatusMap
	prepareFailUntilAttempt  atomic.Int64
	commitFailUntilAttempt   atomic.Int64
	rollbackFailUntilAttempt atomic.Int64
}

func (h *noopTransactionHandler) prepareTransaction(ctx context.Context, transactionID string, preparePayload twopc.PreparePayload) error {
	const method = "prepareTransaction"

	payload, ok := preparePayload.(string)
	if !ok {
		panic(fmt.Sprintf("%s: unexpected payload type %T", method, payload))
	}

	slog.DebugContext(ctx, "called", "method", method, "transactionID", transactionID, "payload", payload)

	if h.shouldFail(ctx, method, &h.prepareFailUntilAttempt) {
		return errSimulatedFailure
	}

	status, ok := h.transactionStatusMap.load(transactionID)
	if ok && status == transactionStatusPrepared {
		slog.DebugContext(ctx, "already prepared", "method", method, "transactionID", transactionID)
		return nil
	}
	if ok {
		return fmt.Errorf("%s: can't prepare transaction, because its status is already %s", method, status)
	}

	h.transactionStatusMap.add(transactionID, transactionStatusPrepared)
	slog.DebugContext(ctx, "prepared", "method", method, "transactionID", transactionID)
	return nil
}

func (h *noopTransactionHandler) commitTransaction(ctx context.Context, transactionID string) error {
	const method = "commitTransaction"

	slog.DebugContext(ctx, "called", "method", method, "transactionID", transactionID)

	if h.shouldFail(ctx, method, &h.commitFailUntilAttempt) {
		return errSimulatedFailure
	}

	status, ok := h.transactionStatusMap.load(transactionID)
	if !ok {
		return fmt.Errorf("%s: transaction for '%s' not found, can't commit unprepared transaction", method, transactionID)
	}
	if status == transactionStatusCommitted {
		slog.DebugContext(ctx, "already committed", "method", method, "transactionID", transactionID)
		return nil
	}
	if status != transactionStatusPrepared {
		return fmt.Errorf("%s: can't commit %s transaction for '%s'", method, status, transactionID)
	}

	h.transactionStatusMap.add(transactionID, transactionStatusCommitted)
	slog.DebugContext(ctx, "committed", "method", method, "transactionID", transactionID)
	return nil
}

func (h *noopTransactionHandler) rollbackTransaction(ctx context.Context, transactionID string) error {
	const method = "rollbackTransaction"

	slog.DebugContext(ctx, "called", "method", method, "transactionID", transactionID)

	if h.shouldFail(ctx, method, &h.rollbackFailUntilAttempt) {
		return errSimulatedFailure
	}

	status, ok := h.transactionStatusMap.load(transactionID)
	if !ok || status == transactionStatusRolledBacked {
		slog.DebugContext(ctx, "already rolled back or not found", "method", method, "transactionID", transactionID)
		return nil
	}
	if status != transactionStatusPrepared {
		return fmt.Errorf("%s: can't rollback %s transaction for '%s'", method, status, transactionID)
	}

	h.transactionStatusMap.add(transactionID, transactionStatusRolledBacked)
	slog.DebugContext(ctx, "rolled back", "method", method, "transactionID", transactionID)
	return nil
}

func (h *noopTransactionHandler) shouldFail(ctx context.Context, method string, counter *atomic.Int64) bool {
	for {
		current := counter.Load()
		if current <= 0 {
			return false
		}
		if counter.CompareAndSwap(current, current-1) {
			slog.DebugContext(ctx, "synthetic fail", "method", method, "remaining fails", current-1)
			return true
		}
	}
}

var errSimulatedFailure = fmt.Errorf("simulated failure")

type transactionStatusMap struct {
	m sync.Map
}

func (tsm *transactionStatusMap) add(transactionID string, transactionStatus transactionStatus) {
	tsm.m.Store(transactionID, transactionStatus)
}

func (tsm *transactionStatusMap) load(transactionID string) (transactionStatus, bool) {
	d, ok := tsm.m.Load(transactionID)
	if !ok {
		return transactionStatusPrepared, false
	}
	return d.(transactionStatus), true
}

type transactionStatus int

const (
	transactionStatusPrepared transactionStatus = iota
	transactionStatusCommitted
	transactionStatusRolledBacked
)

func (s transactionStatus) String() string {
	switch s {
	case transactionStatusPrepared:
		return "prepared"
	case transactionStatusCommitted:
		return "committed"
	case transactionStatusRolledBacked:
		return "rolled_back"
	default:
		return "unknown"
	}
}
