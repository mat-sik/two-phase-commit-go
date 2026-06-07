package adapter

import (
	"io"
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

// TODO: improve error handling and logging
func (h restBasicTransactionPreparer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
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
