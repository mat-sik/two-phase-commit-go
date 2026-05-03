package client

import (
	"context"
	"sync"
)

type PreparePayload interface{}

type Client interface {
	PrepareTransaction(ctx context.Context, transactionID string, payload PreparePayload) error
	CommitTransaction(ctx context.Context, transactionID string) error
	RollbackTransaction(ctx context.Context, transactionID string) error
}

type Registrar struct {
	store     *registrarStore
	newClient func(clientID ID) (Client, error)
}

func NewRegistrar(newClientFunc func(clientID ID) (Client, error)) Registrar {
	return Registrar{
		store:     &registrarStore{},
		newClient: newClientFunc,
	}
}

type ID string

func (cr *Registrar) GetClient(clientID ID) (Client, error) {
	reusableClient, ok := cr.store.load(clientID)
	if !ok {
		newClient, err := cr.newClient(clientID)
		if err != nil {
			return nil, err
		}
		cr.store.add(clientID, newClient)
		reusableClient = newClient
	}
	return reusableClient, nil
}

type registrarStore struct {
	store sync.Map
}

func (store *registrarStore) add(clientID ID, client Client) {
	store.store.Store(clientID, client)
}

func (store *registrarStore) load(clientID ID) (Client, bool) {
	value, ok := store.store.Load(clientID)
	if !ok {
		return nil, false
	}
	return value.(Client), true
}
