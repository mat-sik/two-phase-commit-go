package adapter

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant"
)

func NewTransferMux(pool *pgxpool.Pool) *http.ServeMux {
	handler := participant.NewTransferTransactionHandler(pool)
	return newMux(restHandler{
		transactionPreparer:   restTransferTransactionPreparer{transactionPreparer: handler},
		transactionCommitter:  restTransactionCommitter{transactionCommitter: handler},
		transactionRollbacker: restTransactionRollbacker{transactionRollbacker: handler},
	})
}

type restTransferTransactionPreparer struct {
	transactionPreparer TransferTransactionPreparer
}

func (h restTransferTransactionPreparer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	var payload participant.TransferPayload
	if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
		slog.ErrorContext(ctx, "decoding request body", "err", err)
		http.Error(w, "could not read request", http.StatusInternalServerError)
		return
	}
	transactionID := req.PathValue("transactionID")
	if err := h.transactionPreparer.PrepareTransaction(ctx, transactionID, payload); err != nil {
		slog.ErrorContext(ctx, "preparing tx", "transactionID", transactionID, "err", err)
		http.Error(w, fmt.Sprintf("failed to prepare tx %s", transactionID), http.StatusInternalServerError)
		return
	}
}
