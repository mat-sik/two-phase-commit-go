package coordinator

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type SqlTransactionStateChecker struct {
	Pool *pgxpool.Pool
}

const fetchTransactionStatesQuery = `
	SELECT participant_id, state
	FROM transaction_states
	WHERE transaction_id = $1
`

func (s SqlTransactionStateChecker) Check(ctx context.Context, transactionID string) (map[string]twopc.TransactionState, error) {
	rows, err := s.Pool.Query(ctx, fetchTransactionStatesQuery, transactionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	states := make(map[string]twopc.TransactionState)
	for rows.Next() {
		var participantID string
		var txState twopc.TransactionState
		if err = rows.Scan(&participantID, &txState); err != nil {
			return nil, err
		}
		states[participantID] = txState
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return states, nil
}

type SqlTransactionStatePersister struct {
	Pool *pgxpool.Pool
}

const persistStateQuery = `
    INSERT INTO transaction_states (transaction_id, participant_id, state)
    VALUES ($1, $2, $3)
    ON CONFLICT (transaction_id, participant_id) DO UPDATE
        SET state = EXCLUDED.state
`

func (s SqlTransactionStatePersister) PersistState(
	ctx context.Context,
	transactionID string,
	participantID string,
	transactionState twopc.TransactionState,
) <-chan twopc.PersistResult {
	resultCh := make(chan twopc.PersistResult, 1)

	go func() {
		tx, err := s.Pool.Begin(ctx)
		if err != nil {
			resultCh <- twopc.PersistResult{
				Err: err,
			}
			return
		}
		_, err = tx.Exec(ctx, persistStateQuery, transactionID, participantID, transactionState)
		if err != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil {
				err = errors.Join(err, rollbackErr)
			}
			resultCh <- twopc.PersistResult{
				Err: err,
			}
			return
		}
		resultCh <- successfulPersistResult(ctx, tx)
	}()

	return resultCh
}

func successfulPersistResult(ctx context.Context, tx pgx.Tx) twopc.PersistResult {
	return twopc.PersistResult{
		Commit: func() error {
			return tx.Commit(ctx)
		},
		Rollback: func() error {
			return tx.Rollback(ctx)
		},
		Err: nil,
	}
}
