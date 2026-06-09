package adapter

import (
	"io"
	"log/slog"
	"net/http"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant"
)

func NewBasicMux() *http.ServeMux {
	handler := participant.NewBasicTransactionHandler()
	return newMux(restHandler{
		transactionPreparer:   restBasicTransactionPreparer{transactionPreparer: handler},
		transactionCommitter:  restTransactionCommitter{transactionCommitter: handler},
		transactionRollbacker: restTransactionRollbacker{transactionRollbacker: handler},
	})
}

func NewFailingBasicMux(prepareFailUntilAttempt, commitFailUntilAttempt, rollbackFailUntilAttempt int) *http.ServeMux {
	handler := participant.NewFailingBasicTransactionHandler(prepareFailUntilAttempt, commitFailUntilAttempt, rollbackFailUntilAttempt)
	return newMux(restHandler{
		transactionPreparer:   restBasicTransactionPreparer{transactionPreparer: handler},
		transactionCommitter:  restTransactionCommitter{transactionCommitter: handler},
		transactionRollbacker: restTransactionRollbacker{transactionRollbacker: handler},
	})
}

type restBasicTransactionPreparer struct {
	transactionPreparer BasicTransactionPreparer
}

func (h restBasicTransactionPreparer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	data, err := io.ReadAll(req.Body)
	if err != nil {
		slog.ErrorContext(ctx, "reading request body", "err", err)
		http.Error(w, "could not read request", http.StatusInternalServerError)
		return
	}
	transactionID := req.PathValue("transactionID")
	if err = h.transactionPreparer.PrepareTransaction(ctx, transactionID, string(data)); err != nil {
		slog.ErrorContext(ctx, "preparing tx", "transactionID", transactionID, "err", err)
		http.Error(w, "failed to prepareTransaction", http.StatusInternalServerError)
		return
	}
}
