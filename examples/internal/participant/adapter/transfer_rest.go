package adapter

import (
	"encoding/json"
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
	var payload participant.TransferPayload
	if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
		http.Error(w, "could not read request", http.StatusInternalServerError)
		return
	}

	transactionID := req.PathValue("transactionID")
	if err := h.transactionPreparer.PrepareTransaction(req.Context(), transactionID, payload); err != nil {
		http.Error(w, "failed to prepareTransaction", http.StatusInternalServerError)
		return
	}
}
