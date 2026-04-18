package coordinator

type transactionState int

const (
	transactionNotStarted transactionState = iota
	transactionPrepared
	transactionPrepareFailed
	transactionCommitted
	transactionRolledBack
)
