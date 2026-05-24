package coordinator

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type SqlStatePersister struct {
	pool *pgxpool.Pool
}

const persistStateQuery = `
    INSERT INTO transaction_states (transaction_id, participant_id, state)
    VALUES ($1, $2, $3)
    ON CONFLICT (transaction_id, participant_id) DO UPDATE
        SET state = EXCLUDED.state
`

func (s SqlStatePersister) PersistState(
	ctx context.Context,
	transactionID string,
	participantID string,
	transactionState twopc.TransactionState,
) <-chan twopc.PersistResult {
	resultCh := make(chan twopc.PersistResult, 1)

	go func() {
		tx, err := s.pool.Begin(ctx)
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
