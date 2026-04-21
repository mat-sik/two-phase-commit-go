package client

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

func NewNoopHandler() *Handler {
	handler := &noopTransactionHandler{
		transactionStatusMap: &transactionStatusMap{},
	}
	return &Handler{
		transactionPreparer:   handler,
		transactionCommiter:   handler,
		transactionRollbacker: handler,
	}
}

type noopTransactionHandler struct {
	transactionStatusMap *transactionStatusMap
}

func (n *noopTransactionHandler) prepareTransaction(_ context.Context, transactionID string, body string) (bool, error) {
	status, ok := n.transactionStatusMap.load(transactionID)
	if ok && status == transactionStatusPrepared {
		return true, nil
	}
	if ok {
		return false, fmt.Errorf("can't prepare transaction, because its status is already %s", status)
	}

	n._prepareTransaction(transactionID, body)
	return true, nil
}

func (n *noopTransactionHandler) _prepareTransaction(transactionID string, body string) {
	slog.Info("preparing transaction", slog.String("transactionID", transactionID), slog.String("body", body))
	time.Sleep(1 * time.Second)
	n.transactionStatusMap.add(transactionID, transactionStatusPrepared)
	slog.Info("prepared transaction")
}

func (n *noopTransactionHandler) commitTransaction(_ context.Context, transactionID string) (bool, error) {
	status, ok := n.transactionStatusMap.load(transactionID)
	if !ok {
		return false, fmt.Errorf("transaction for '%s' not found, can't commit unprepared transaction", transactionID)
	}
	if status == transactionStatusCommited {
		return true, nil
	}
	if status != transactionStatusPrepared {
		return false, fmt.Errorf("can't commit %s transaction for '%s'", status, transactionID)
	}
	n._commitTransaction(transactionID)
	return true, nil
}

func (n *noopTransactionHandler) _commitTransaction(transactionID string) {
	slog.Info("committing transaction", slog.String("transactionID", transactionID))
	n.transactionStatusMap.add(transactionID, transactionStatusCommited)
}

func (n *noopTransactionHandler) rollbackTransaction(_ context.Context, transactionID string) (bool, error) {
	status, ok := n.transactionStatusMap.load(transactionID)
	if !ok {
		return false, fmt.Errorf("transaction for '%s' not found, can't rollback unprepared transaction", transactionID)
	}
	if status == transactionStatusRolledBacked {
		return true, nil
	}
	if status != transactionStatusPrepared {
		return false, fmt.Errorf("can't rollback %s transaction for '%s'", status, transactionID)
	}
	n._rollbackTransaction(transactionID)
	return true, nil
}

func (n *noopTransactionHandler) _rollbackTransaction(transactionID string) {
	slog.Info("rolling back transaction", slog.String("transactionID", transactionID))
	n.transactionStatusMap.add(transactionID, transactionStatusRolledBacked)
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
	transactionStatusCommited
	transactionStatusRolledBacked
)

func (s transactionStatus) String() string {
	switch s {
	case transactionStatusPrepared:
		return "prepared"
	case transactionStatusCommited:
		return "committed"
	case transactionStatusRolledBacked:
		return "rolled_back"
	default:
		return "unknown"
	}
}
