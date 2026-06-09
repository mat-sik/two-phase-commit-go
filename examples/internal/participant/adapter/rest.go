package adapter

import (
	"fmt"
	"log/slog"
	"net/http"
)

func newMux(transactionHandlers restHandler) *http.ServeMux {
	mux := http.NewServeMux()

	mux.Handle("POST /transactions/{transactionID}/prepare", transactionHandlers.transactionPreparer)
	mux.Handle("POST /transactions/{transactionID}/commit", transactionHandlers.transactionCommitter)
	mux.Handle("POST /transactions/{transactionID}/rollback", transactionHandlers.transactionRollbacker)

	return mux
}

type restHandler struct {
	transactionPreparer   http.Handler
	transactionCommitter  http.Handler
	transactionRollbacker http.Handler
}

type restTransactionCommitter struct {
	transactionCommitter TransactionCommiter
}

func (h restTransactionCommitter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	transactionID := req.PathValue("transactionID")
	if err := h.transactionCommitter.CommitTransaction(req.Context(), transactionID); err != nil {
		slog.ErrorContext(ctx, "committing tx", "transactionID", transactionID, "err", err)
		http.Error(w, fmt.Sprintf("failed to commit tx %s", transactionID), http.StatusInternalServerError)
		return
	}
}

type restTransactionRollbacker struct {
	transactionRollbacker TransactionRollbacker
}

func (h restTransactionRollbacker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	transactionID := req.PathValue("transactionID")
	if err := h.transactionRollbacker.RollbackTransaction(ctx, transactionID); err != nil {
		slog.ErrorContext(ctx, "rolling back tx", "transactionID", transactionID, "err", err)
		http.Error(w, fmt.Sprintf("failed to rollback tx %s", transactionID), http.StatusInternalServerError)
		return
	}
}
