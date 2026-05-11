package twopc

// DistributedTransaction groups all participant transactions that should be
// committed or rolled back atomically under a single coordinator-assigned ID.
type DistributedTransaction[ID comparable] struct {
	// TransactionID uniquely identifies this distributed transaction across all
	// participants and coordinator restarts. It is used as the key for state
	// persistence and recovery.
	TransactionID string
	// Transactions is the list of per-participant operations that form the
	// distributed transaction. Each entry names a participant and carries the
	// payload to be forwarded during the Prepare phase.
	Transactions []Transaction[ID]
}

// Transaction describes a single participant's involvement in a distributed
// transaction.
type Transaction[ID comparable] struct {
	// ClientID identifies the participant. It must match a client registered
	// via the newClientFunc passed to NewCoordinator.
	ClientID ID
	// Payload is the opaque data forwarded to the participant during the
	// Prepare phase. The coordinator does not interpret its contents.
	Payload PreparePayload
}
