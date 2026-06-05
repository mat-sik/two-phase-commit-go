package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type sqlTransactionHandler struct {
	pool *pgxpool.Pool
}

type TransferPayload struct {
	SenderID   string  `json:"sender_id"`
	ReceiverID string  `json:"receiver_id"`
	Amount     float64 `json:"amount"`
}

func (h *sqlTransactionHandler) prepareTransaction(ctx context.Context, transactionID string, preparePayload twopc.PreparePayload) (err error) {
	const method = "prepareTransaction"

	payload, ok := preparePayload.(TransferPayload)
	if !ok {
		panic(fmt.Sprintf("%s: unexpected payload type %T", method, payload))
	}

	slog.DebugContext(ctx, "called", "method", method, "transactionID", transactionID, "payload", payload)

	var tx pgx.Tx
	tx, err = h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%s: begin: %w", method, err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); err != nil {
			err = errors.Join(err, rollbackErr)
		} else {
			err = rollbackErr
		}
	}()

	var exists bool
	exists, err = isPrepared(ctx, tx, transactionID)
	if err != nil {
		return fmt.Errorf("%s: check prepared: %w", method, err)
	}
	if exists {
		slog.DebugContext(ctx, "already prepared", "method", method, "transactionID", transactionID)
		return nil
	}

	if err = transferFunds(ctx, tx, payload); err != nil {
		return fmt.Errorf("%s: transfer funds: %w", method, err)
	}

	if err = insertTransferLog(ctx, tx, transactionID, payload.SenderID, payload.ReceiverID, payload.Amount, statusPending); err != nil {
		return fmt.Errorf("%s: insert log: %w", method, err)
	}

	if err = prepareTransaction(ctx, tx, transactionID); err != nil {
		return fmt.Errorf("%s: prepare transaction: %w", method, err)
	}

	slog.DebugContext(ctx, "prepared", "method", method, "transactionID", transactionID)

	return nil
}

func transferFunds(ctx context.Context, tx pgx.Tx, payload TransferPayload) error {
	const method = "transferFunds"

	if err := upsertAccount(ctx, tx, -payload.Amount, payload.SenderID); err != nil {
		return fmt.Errorf("%s: debit sender: %w", method, err)
	}

	if err := upsertAccount(ctx, tx, payload.Amount, payload.ReceiverID); err != nil {
		return fmt.Errorf("%s: credit receiver: %w", method, err)
	}

	return nil
}

func upsertAccount(ctx context.Context, tx pgx.Tx, amount float64, accountID string) error {
	const upsertAccountQuery = `
		INSERT INTO accounts (id, balance)
		VALUES ($2, $1)
		ON CONFLICT (id)
		DO UPDATE SET balance = accounts.balance + $1
	`
	_, err := tx.Exec(ctx, upsertAccountQuery, amount, accountID)
	return err
}

func prepareTransaction(ctx context.Context, tx pgx.Tx, transactionID string) error {
	const prepareTransactionQuery = "PREPARE TRANSACTION $1"
	_, err := tx.Exec(ctx, prepareTransactionQuery, transactionID)
	return err
}

func (h *sqlTransactionHandler) commitTransaction(ctx context.Context, transactionID string) error {
	const method = "commitTransaction"

	slog.DebugContext(ctx, "called", "method", method, "transactionID", transactionID)

	conn, err := h.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("%s: acquire connection: %w", method, err)
	}
	defer conn.Release()

	var exists bool
	exists, err = isPrepared(ctx, conn, transactionID)
	if err != nil {
		return fmt.Errorf("%s: check prepared: %w", method, err)
	}
	if !exists {
		slog.DebugContext(ctx, "already committed", "method", method, "transactionID", transactionID)
		return nil
	}

	if err = commitTransaction(ctx, conn, transactionID); err != nil {
		return fmt.Errorf("%s: commit prepared: %w", method, err)
	}

	if err = insertAuditLog(ctx, conn, transactionID, statusCommitted); err != nil {
		return fmt.Errorf("%s: insert audit log: %w", method, err)
	}

	slog.DebugContext(ctx, "committed", "method", method, "transactionID", transactionID)

	return nil
}

func commitTransaction(ctx context.Context, conn *pgxpool.Conn, transactionID string) error {
	const commitPreparedQuery = "COMMIT PREPARED $1"
	_, err := conn.Exec(ctx, commitPreparedQuery, transactionID)
	return err
}

func (h *sqlTransactionHandler) rollbackTransaction(ctx context.Context, transactionID string) error {
	const method = "rollbackTransaction"

	slog.DebugContext(ctx, "called", "method", method, "transactionID", transactionID)

	conn, err := h.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("%s: acquire connection: %w", method, err)
	}
	defer conn.Release()

	var exists bool
	exists, err = isPrepared(ctx, conn, transactionID)
	if err != nil {
		return fmt.Errorf("%s: check prepared: %w", method, err)
	}
	if !exists {
		slog.DebugContext(ctx, "already rolled back", "method", method, "transactionID", transactionID)
		return nil
	}

	if err = rollbackTransaction(ctx, conn, transactionID); err != nil {
		return fmt.Errorf("%s: rollback prepared: %w", method, err)
	}

	if err = insertAuditLog(ctx, conn, transactionID, statusRolledBack); err != nil {
		return fmt.Errorf("%s: insert audit log: %w", method, err)
	}

	slog.DebugContext(ctx, "rolled back", "method", method, "transactionID", transactionID)

	return nil
}

func rollbackTransaction(ctx context.Context, conn *pgxpool.Conn, transactionID string) error {
	const rollbackPreparedQuery = "ROLLBACK PREPARED $1"
	_, err := conn.Exec(ctx, rollbackPreparedQuery, transactionID)
	return err
}

type execQuerier interface {
	querier
	execer
}

func insertAuditLog(ctx context.Context, execQuerier execQuerier, transactionID string, status transferStatus) error {
	const method = "insertAuditLog"

	senderID, receiverID, amount, err := selectTransferLog(ctx, execQuerier, transactionID, statusPending)
	if err != nil {
		return fmt.Errorf("%s: read pending log: %w", method, err)
	}

	if err = insertTransferLog(ctx, execQuerier, transactionID, senderID, receiverID, amount, status); err != nil {
		return fmt.Errorf("%s: insert log: %w", method, err)
	}

	return nil
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func selectTransferLog(ctx context.Context, q querier, transactionID string, status transferStatus) (senderID, receiverID string, amount float64, err error) {
	const selectTransferLogQuery = `
		SELECT sender_id, receiver_id, amount
		FROM transfer_log
		WHERE transaction_id = $1
		AND status = $2
	`
	err = q.QueryRow(ctx, selectTransferLogQuery, transactionID, status).Scan(&senderID, &receiverID, &amount)
	return
}

type execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func insertTransferLog(ctx context.Context, e execer, transactionID, senderID, receiverID string, amount float64, status transferStatus) error {
	const insertTransferLogQuery = `
		INSERT INTO transfer_log (transaction_id, sender_id, receiver_id, amount, status)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := e.Exec(ctx, insertTransferLogQuery, transactionID, senderID, receiverID, amount, status)
	return err
}

func isPrepared(ctx context.Context, q querier, transactionID string) (bool, error) {
	const existsPreparedQuery = "SELECT EXISTS(SELECT 1 FROM pg_prepared_xacts WHERE gid = $1)"
	var exists bool
	err := q.QueryRow(ctx, existsPreparedQuery, transactionID).Scan(&exists)
	return exists, err
}

type transferStatus int

const (
	statusPending transferStatus = iota
	statusCommitted
	statusRolledBack
)
