package adapter

import "net/http"

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
	transactionID := req.PathValue("transactionID")
	err := h.transactionCommitter.CommitTransaction(req.Context(), transactionID)
	if err != nil {
		http.Error(w, "failed to commitTransaction", http.StatusInternalServerError)
		return
	}
}

type restTransactionRollbacker struct {
	transactionRollbacker TransactionRollbacker
}

func (h restTransactionRollbacker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	transactionID := req.PathValue("transactionID")
	err := h.transactionRollbacker.RollbackTransaction(req.Context(), transactionID)
	if err != nil {
		http.Error(w, "failed to rollbackTransaction", http.StatusInternalServerError)
		return
	}
}
