package client

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

type noopTransactionHandler struct {
	transactionStatusMap     *transactionStatusMap
	prepareFailUntilAttempt  atomic.Int64
	commitFailUntilAttempt   atomic.Int64
	rollbackFailUntilAttempt atomic.Int64
}

func (n *noopTransactionHandler) prepareTransaction(ctx context.Context, transactionID string, payload string) error {
	slog.DebugContext(ctx, "prepareTransaction called", "transactionID", transactionID, "body", payload)
	if n.shouldFail(ctx, &n.prepareFailUntilAttempt) {
		return errSimulatedFailure
	}
	status, ok := n.transactionStatusMap.load(transactionID)
	if ok && status == transactionStatusPrepared {
		slog.DebugContext(ctx, "transaction already prepared")
		return nil
	}
	if ok {
		return fmt.Errorf("can't prepare transaction, because its status is already %s", status)
	}
	n.storePrepared(ctx, transactionID, payload)
	return nil
}

func (n *noopTransactionHandler) storePrepared(ctx context.Context, transactionID string, body string) {
	slog.DebugContext(ctx, "preparing transaction", slog.String("transactionID", transactionID), slog.String("body", body))
	n.transactionStatusMap.add(transactionID, transactionStatusPrepared)
	slog.DebugContext(ctx, "prepared transaction")
}

func (n *noopTransactionHandler) commitTransaction(ctx context.Context, transactionID string) error {
	slog.DebugContext(ctx, "commitTransaction called", "transactionID", transactionID)
	if n.shouldFail(ctx, &n.commitFailUntilAttempt) {
		return errSimulatedFailure
	}
	status, ok := n.transactionStatusMap.load(transactionID)
	if !ok {
		return fmt.Errorf("transaction for '%s' not found, can't commit unprepared transaction", transactionID)
	}
	if status == transactionStatusCommitted {
		return nil
	}
	if status != transactionStatusPrepared {
		return fmt.Errorf("can't commit %s transaction for '%s'", status, transactionID)
	}
	n.storeCommitted(ctx, transactionID)
	return nil
}

func (n *noopTransactionHandler) storeCommitted(ctx context.Context, transactionID string) {
	slog.DebugContext(ctx, "committing transaction", slog.String("transactionID", transactionID))
	n.transactionStatusMap.add(transactionID, transactionStatusCommitted)
	slog.DebugContext(ctx, "committed transaction")
}

func (n *noopTransactionHandler) rollbackTransaction(ctx context.Context, transactionID string) error {
	slog.DebugContext(ctx, "rollbackTransaction called", "transactionID", transactionID)
	if n.shouldFail(ctx, &n.rollbackFailUntilAttempt) {
		return errSimulatedFailure
	}
	status, ok := n.transactionStatusMap.load(transactionID)
	if !ok || status == transactionStatusRolledBacked {
		return nil
	}
	if status != transactionStatusPrepared {
		return fmt.Errorf("can't rollback %s transaction for '%s'", status, transactionID)
	}
	n.storeRolledBack(ctx, transactionID)
	return nil
}

func (n *noopTransactionHandler) shouldFail(ctx context.Context, counter *atomic.Int64) bool {
	for {
		current := counter.Load()
		if current <= 0 {
			return false
		}
		if counter.CompareAndSwap(current, current-1) {
			slog.DebugContext(ctx, "synthetic fail", "remaining fails", current-1)
			return true
		}
	}
}

var errSimulatedFailure = fmt.Errorf("simulated failure")

func (n *noopTransactionHandler) storeRolledBack(ctx context.Context, transactionID string) {
	slog.DebugContext(ctx, "rolling back transaction", slog.String("transactionID", transactionID))
	n.transactionStatusMap.add(transactionID, transactionStatusRolledBacked)
	slog.DebugContext(ctx, "rollback transaction")
}

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
