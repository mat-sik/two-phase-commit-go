package twopc

import "github.com/mat-sik/two-phase-commit-go/twopc/internal/client"

type DistributedTransaction struct {
	TransactionID string
	Transactions  []Transaction
}

type Transaction struct {
	ClientID client.ID
	Payload  string
}

func (t Transaction) ClientIdentifier() client.ID {
	return t.ClientID
}

func NewTransaction(clientIDString string, payload string) Transaction {
	return Transaction{
		ClientID: client.ID(clientIDString),
		Payload:  payload,
	}
}
