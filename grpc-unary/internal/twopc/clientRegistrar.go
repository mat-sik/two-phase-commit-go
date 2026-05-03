package twopc

import (
	"context"
	"sync"
)

type Client interface {
	prepareTransaction(ctx context.Context, transactionID string, operation prepareOperation) error
	commitTransaction(ctx context.Context, transactionID string) error
	rollbackTransaction(ctx context.Context, transactionID string) error
}

type clientRegistrar struct {
	store     *clientRegistrarStore
	newClient func(identifiable ClientRegistrarUsable) (Client, error)
}

func newClientRegistrar(newClientFunc func(identifiable ClientRegistrarUsable) (Client, error)) clientRegistrar {
	return clientRegistrar{
		store:     &clientRegistrarStore{},
		newClient: newClientFunc,
	}
}

type ClientID string

type ClientRegistrarUsable interface {
	ClientIdentifier() ClientID
}

func (cr *clientRegistrar) getClient(identifiable ClientRegistrarUsable) (Client, error) {
	reusableClient, ok := cr.store.load(identifiable.ClientIdentifier())
	if !ok {
		newClient, err := cr.newClient(identifiable)
		if err != nil {
			return nil, err
		}
		cr.store.add(identifiable.ClientIdentifier(), newClient)
		reusableClient = newClient
	}
	return reusableClient, nil
}

type clientRegistrarStore struct {
	store sync.Map
}

func (store *clientRegistrarStore) add(clientID ClientID, client Client) {
	store.store.Store(clientID, client)
}

func (store *clientRegistrarStore) load(clientID ClientID) (Client, bool) {
	value, ok := store.store.Load(clientID)
	if !ok {
		return nil, false
	}
	return value.(Client), true
}
