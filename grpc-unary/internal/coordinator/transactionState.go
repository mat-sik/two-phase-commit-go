package coordinator

type TransactionState int

const (
	transactionNotStarted TransactionState = iota
	transactionPrepared
	transactionPrepareFailed
	transactionCommitted
	transactionRolledBack
)
