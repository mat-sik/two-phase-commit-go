package persister

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type PostgresTransactionStateChecker struct {
	pool *pgxpool.Pool
}

func NewPostgresTransactionStateChecker(pool *pgxpool.Pool) PostgresTransactionStateChecker {
	return PostgresTransactionStateChecker{
		pool: pool,
	}
}

func (s PostgresTransactionStateChecker) Check(ctx context.Context, transactionID string) (map[string]twopc.TransactionState, error) {
	const fetchTransactionStatesQuery = `
		SELECT participant_id, state
		FROM transaction_states
		WHERE transaction_id = $1
	`
	rows, err := s.pool.Query(ctx, fetchTransactionStatesQuery, transactionID)
	if err != nil {
		return nil, fmt.Errorf("querying tx %s state rows: %w", transactionID, err)
	}
	defer rows.Close()

	states := make(map[string]twopc.TransactionState)
	for rows.Next() {
		var participantID string
		var txState twopc.TransactionState
		if err = rows.Scan(&participantID, &txState); err != nil {
			return nil, fmt.Errorf("scanning tx %s state row: %w", transactionID, err)
		}
		states[participantID] = txState
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating tx %s state rows: %w", transactionID, err)
	}
	return states, nil
}

type PostgresTransactionStatePersister struct {
	pool *pgxpool.Pool
}

func NewPostgresTransactionStatePersister(pool *pgxpool.Pool) PostgresTransactionStatePersister {
	return PostgresTransactionStatePersister{
		pool: pool,
	}
}

func (s PostgresTransactionStatePersister) PersistState(
	ctx context.Context,
	transactionID string,
	participantID string,
	transactionState twopc.TransactionState,
) (err error) {
	var tx pgx.Tx
	tx, err = s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf(
			"beginning pgx tx for persisting tx %s state %d for participant %s: %w",
			transactionID, transactionState, participantID, err,
		)
	}
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				err = errors.Join(err, fmt.Errorf(
					"rolling back pgx tx for persisting tx %s state %d for participant %s: %w",
					transactionID, transactionState, participantID, rollbackErr,
				))
			}
		}
	}()

	const persistStateQuery = `
			INSERT INTO transaction_states (transaction_id, participant_id, state)
			VALUES ($1, $2, $3)
			ON CONFLICT (transaction_id, participant_id) DO UPDATE
				SET state = EXCLUDED.state
	`
	if _, err = tx.Exec(ctx, persistStateQuery, transactionID, participantID, transactionState); err != nil {
		return fmt.Errorf(
			"persisting tx %s state %d for participant %s: %w",
			transactionID, transactionState, participantID, err,
		)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"committing tx %s state %d for participant %s: %w",
			transactionID, transactionState, participantID, err,
		)
	}
	return nil
}
