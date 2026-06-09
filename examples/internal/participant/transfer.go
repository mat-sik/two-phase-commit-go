package participant

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TransferTransactionHandler struct {
	pool *pgxpool.Pool
}

func NewTransferTransactionHandler(pool *pgxpool.Pool) *TransferTransactionHandler {
	return &TransferTransactionHandler{
		pool: pool,
	}
}

type TransferPayload struct {
	SenderID   string  `json:"sender_id"`
	ReceiverID string  `json:"receiver_id"`
	Amount     float64 `json:"amount"`
}

func (h *TransferTransactionHandler) PrepareTransaction(ctx context.Context, transactionID string, payload TransferPayload) (err error) {
	slog.DebugContext(ctx, "prepare transfer", "transactionID", transactionID, "payload", payload)

	var tx pgx.Tx
	tx, err = h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning pgx tx for preparing tx %s: %w", transactionID, err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			rollbackErr = fmt.Errorf("rolling back tx %s: %w", transactionID, rollbackErr)
			if err != nil {
				err = errors.Join(err, rollbackErr)
			} else {
				err = rollbackErr
			}
		}
	}()

	var exists bool
	exists, err = isPrepared(ctx, tx, transactionID)
	if err != nil {
		return err
	}
	if exists {
		slog.DebugContext(ctx, "already prepared", "transactionID", transactionID)
		return nil
	}

	if err = transferFunds(ctx, tx, payload); err != nil {
		return err
	}

	if err = insertTransferLog(ctx, tx, transactionID, payload.SenderID, payload.ReceiverID, payload.Amount, transferStatusPending); err != nil {
		return
	}

	if err = prepareTransaction(ctx, tx, transactionID); err != nil {
		return err
	}

	slog.DebugContext(ctx, "prepared", "transactionID", transactionID)

	return nil
}

func transferFunds(ctx context.Context, tx pgx.Tx, payload TransferPayload) error {
	if err := upsertAccount(ctx, tx, payload.SenderID, -payload.Amount); err != nil {
		return err
	}
	if err := upsertAccount(ctx, tx, payload.ReceiverID, payload.Amount); err != nil {
		return err
	}
	return nil
}

func upsertAccount(ctx context.Context, tx pgx.Tx, accountID string, amount float64) error {
	const upsertAccountQuery = `
		INSERT INTO accounts (id, balance)
		VALUES ($2, $1)
		ON CONFLICT (id)
		DO UPDATE SET balance = accounts.balance + $1
	`
	if _, err := tx.Exec(ctx, upsertAccountQuery, amount, accountID); err != nil {
		return fmt.Errorf("upserting account %s for %f: %w", accountID, amount, err)
	}
	return nil
}

func prepareTransaction(ctx context.Context, tx pgx.Tx, transactionID string) error {
	prepareTransactionQuery := fmt.Sprintf(`PREPARE TRANSACTION '%s'`, transactionID)
	if _, err := tx.Exec(ctx, prepareTransactionQuery); err != nil {
		return fmt.Errorf("prepare transaction: %w", err)
	}
	return nil
}

func (h *TransferTransactionHandler) CommitTransaction(ctx context.Context, transactionID string) error {
	slog.DebugContext(ctx, "commit transfer", "transactionID", transactionID)

	conn, err := h.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring conn for commiting tx %s: %w", transactionID, err)
	}
	defer conn.Release()

	var exists bool
	exists, err = isPrepared(ctx, conn, transactionID)
	if err != nil {
		return err
	}
	if !exists {
		slog.DebugContext(ctx, "already committed", "transactionID", transactionID)
		return nil
	}

	if err = commitTransaction(ctx, conn, transactionID); err != nil {
		return err
	}

	if err = insertAuditLog(ctx, conn, transactionID, transferStatusCommitted); err != nil {
		return err
	}

	slog.DebugContext(ctx, "committed", "transactionID", transactionID)

	return nil
}

func commitTransaction(ctx context.Context, conn *pgxpool.Conn, transactionID string) error {
	commitPreparedQuery := fmt.Sprintf(`COMMIT PREPARED '%s'`, transactionID)
	if _, err := conn.Exec(ctx, commitPreparedQuery); err != nil {
		return fmt.Errorf("commiting prepared tx %s: %w", transactionID, err)
	}
	return nil
}

func (h *TransferTransactionHandler) RollbackTransaction(ctx context.Context, transactionID string) error {
	slog.DebugContext(ctx, "rollback", "transactionID", transactionID)

	conn, err := h.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring conn for rolling back tx %s : %w", transactionID, err)
	}
	defer conn.Release()

	var exists bool
	exists, err = isPrepared(ctx, conn, transactionID)
	if err != nil {
		return err
	}
	if !exists {
		slog.DebugContext(ctx, "already rolled back", "transactionID", transactionID)
		return nil
	}

	if err = rollbackTransaction(ctx, conn, transactionID); err != nil {
		return err
	}

	if err = insertAuditLog(ctx, conn, transactionID, transferStatusRolledBack); err != nil {
		return err
	}

	slog.DebugContext(ctx, "rolled back", "transactionID", transactionID)

	return nil
}

func rollbackTransaction(ctx context.Context, conn *pgxpool.Conn, transactionID string) error {
	rollbackPreparedQuery := fmt.Sprintf(`ROLLBACK PREPARED '%s'`, transactionID)
	_, err := conn.Exec(ctx, rollbackPreparedQuery)
	if err != nil {
		return fmt.Errorf("rolling back prepared tx %s: %w", transactionID, err)
	}
	return nil
}

type execQuerier interface {
	querier
	execer
}

func insertAuditLog(ctx context.Context, execQuerier execQuerier, transactionID string, status transferStatus) error {
	senderID, receiverID, amount, err := selectTransferLog(ctx, execQuerier, transactionID, transferStatusPending)
	if err != nil {
		return err
	}
	if err = insertTransferLog(ctx, execQuerier, transactionID, senderID, receiverID, amount, status); err != nil {
		return err
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
	if err = q.QueryRow(ctx, selectTransferLogQuery, transactionID, status).Scan(&senderID, &receiverID, &amount); err != nil {
		err = fmt.Errorf("selecting log tx %s status %d: %w", transactionID, status, err)
	}
	return
}

type execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func insertTransferLog(
	ctx context.Context,
	e execer,
	transactionID, senderID, receiverID string,
	amount float64,
	status transferStatus,
) error {
	const insertTransferLogQuery = `
		INSERT INTO transfer_log (transaction_id, sender_id, receiver_id, amount, status)
		VALUES ($1, $2, $3, $4, $5)
	`
	if _, err := e.Exec(ctx, insertTransferLogQuery, transactionID, senderID, receiverID, amount, status); err != nil {
		return fmt.Errorf(
			"inserting log tx %s sender %s receiver %s amount %f status %d: %w",
			transactionID, senderID, receiverID, amount, status, err,
		)
	}
	return nil
}

func isPrepared(ctx context.Context, q querier, transactionID string) (bool, error) {
	const existsPreparedQuery = "SELECT EXISTS(SELECT 1 FROM pg_prepared_xacts WHERE gid = $1)"
	var exists bool
	if err := q.QueryRow(ctx, existsPreparedQuery, transactionID).Scan(&exists); err != nil {
		return false, fmt.Errorf("is tx %s prepared: %w", transactionID, err)
	}
	return exists, nil
}

type transferStatus int

const (
	transferStatusPending transferStatus = iota
	transferStatusCommitted
	transferStatusRolledBack
)
