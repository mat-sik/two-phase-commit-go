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

type Registrar[ID comparable] struct {
	store     *registrarStore[ID]
	newClient func(clientID ID) (Client, error)
}

func NewRegistrar[ID comparable](newClientFunc func(clientID ID) (Client, error)) Registrar[ID] {
	return Registrar[ID]{
		store:     &registrarStore[ID]{},
		newClient: newClientFunc,
	}
}

func (cr *Registrar[ID]) GetClient(clientID ID) (Client, error) {
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

type registrarStore[ID comparable] struct {
	store sync.Map
}

func (s *registrarStore[ID]) add(id ID, client Client) {
	s.store.Store(id, client)
}

func (s *registrarStore[ID]) load(id ID) (Client, bool) {
	value, ok := s.store.Load(id)
	if !ok {
		return nil, false
	}
	return value.(Client), true
}

type ID string
