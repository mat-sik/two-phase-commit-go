package twopc

type DistributedTransaction[ID comparable] struct {
	TransactionID string
	Transactions  []Transaction[ID]
}

type Transaction[ID comparable] struct {
	ClientID ID
	Payload  string
}

func NewTransaction[ID comparable](clientID ID, payload string) Transaction[ID] {
	return Transaction[ID]{
		ClientID: clientID,
		Payload:  payload,
	}
}
