package client

import (
	"io"
	"net/http"
	"sync/atomic"
)

func NewNoopMux() *http.ServeMux {
	handler := &noopTransactionHandler{
		transactionStatusMap:     &transactionStatusMap{},
		prepareFailUntilAttempt:  atomic.Int64{},
		commitFailUntilAttempt:   atomic.Int64{},
		rollbackFailUntilAttempt: atomic.Int64{},
	}
	handler.prepareFailUntilAttempt.Store(0)
	handler.commitFailUntilAttempt.Store(0)
	handler.rollbackFailUntilAttempt.Store(0)
	return newMux(restHandler{
		transactionPreparer:   restTransactionPreparer{transactionPreparer: handler},
		transactionCommitter:  restTransactionCommitter{transactionCommitter: handler},
		transactionRollbacker: restTransactionRollbacker{transactionRollbacker: handler},
	})
}

func NewFailingNoopMux(prepareFailUntilAttempt, commitFailUntilAttempt, rollbackFailUntilAttempt int) *http.ServeMux {
	handler := &noopTransactionHandler{
		transactionStatusMap:     &transactionStatusMap{},
		prepareFailUntilAttempt:  atomic.Int64{},
		commitFailUntilAttempt:   atomic.Int64{},
		rollbackFailUntilAttempt: atomic.Int64{},
	}
	handler.prepareFailUntilAttempt.Store(int64(prepareFailUntilAttempt))
	handler.commitFailUntilAttempt.Store(int64(commitFailUntilAttempt))
	handler.rollbackFailUntilAttempt.Store(int64(rollbackFailUntilAttempt))
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
	transactionPreparer transactionPreparer
}

func (h *restTransactionPreparer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	data, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "could not read request", http.StatusInternalServerError)
		return
	}

	transactionID := req.PathValue("transactionID")
	err = h.transactionPreparer.prepareTransaction(req.Context(), transactionID, string(data))
	if err != nil {
		http.Error(w, "failed to prepareTransaction", http.StatusInternalServerError)
		return
	}
}

type restTransactionCommitter struct {
	transactionCommitter transactionCommiter
}

func (h restTransactionCommitter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	transactionID := req.PathValue("transactionID")
	err := h.transactionCommitter.commitTransaction(req.Context(), transactionID)
	if err != nil {
		http.Error(w, "failed to commitTransaction", http.StatusInternalServerError)
		return
	}
}

type restTransactionRollbacker struct {
	transactionRollbacker transactionRollbacker
}

func (h restTransactionRollbacker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	transactionID := req.PathValue("transactionID")
	err := h.transactionRollbacker.rollbackTransaction(req.Context(), transactionID)
	if err != nil {
		http.Error(w, "failed to rollbackTransaction", http.StatusInternalServerError)
		return
	}
}
