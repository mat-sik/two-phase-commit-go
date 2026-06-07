package adapter

import (
	"io"
	"net/http"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
)

func NewBasicMux() *http.ServeMux {
	handler := client.NewBasicTransactionHandler()
	return newMux(restHandler{
		transactionPreparer:   restTransactionPreparer{transactionPreparer: handler},
		transactionCommitter:  restTransactionCommitter{transactionCommitter: handler},
		transactionRollbacker: restTransactionRollbacker{transactionRollbacker: handler},
	})
}

func NewFailingBasicMux(prepareFailUntilAttempt, commitFailUntilAttempt, rollbackFailUntilAttempt int) *http.ServeMux {
	handler := client.NewFailingBasicTransactionHandler(prepareFailUntilAttempt, commitFailUntilAttempt, rollbackFailUntilAttempt)
	return newMux(restHandler{
		transactionPreparer:   restTransactionPreparer{transactionPreparer: handler},
		transactionCommitter:  restTransactionCommitter{transactionCommitter: handler},
		transactionRollbacker: restTransactionRollbacker{transactionRollbacker: handler},
	})
}

func newMux(transactionHandlers restHandler) *http.ServeMux {
	mux := http.NewServeMux()

	mux.Handle("POST /transactions/{transactionID}/prepare", &transactionHandlers.transactionPreparer)
	mux.Handle("POST /transactions/{transactionID}/commit", &transactionHandlers.transactionCommitter)
	mux.Handle("POST /transactions/{transactionID}/rollback", &transactionHandlers.transactionRollbacker)

	return mux
}

type restHandler struct {
	transactionPreparer   restTransactionPreparer
	transactionCommitter  restTransactionCommitter
	transactionRollbacker restTransactionRollbacker
}

type restTransactionPreparer struct {
	transactionPreparer BasicTransactionPreparer
}

func (h *restTransactionPreparer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	data, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "could not read request", http.StatusInternalServerError)
		return
	}

	transactionID := req.PathValue("transactionID")
	err = h.transactionPreparer.PrepareTransaction(req.Context(), transactionID, string(data))
	if err != nil {
		http.Error(w, "failed to prepareTransaction", http.StatusInternalServerError)
		return
	}
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
