package twopc

type DistributedTransaction[ID comparable] struct {
	TransactionID string
	Transactions  []Transaction[ID]
}

type Transaction[ID comparable] struct {
	ClientID ID
	Payload  PreparePayload
}
