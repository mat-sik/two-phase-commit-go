package participant

import (
	"context"
	"sync"
)

type PreparePayload any

type Client interface {
	PrepareTransaction(ctx context.Context, transactionID string, payload PreparePayload) error
	CommitTransaction(ctx context.Context, transactionID string) error
	RollbackTransaction(ctx context.Context, transactionID string) error
}

type Registrar[ID comparable] struct {
	store     *registrarStore[ID]
	newClient func(participantID ID) (Client, error)
}

func NewRegistrar[ID comparable](newClientFunc func(participantID ID) (Client, error)) Registrar[ID] {
	return Registrar[ID]{
		store:     &registrarStore[ID]{},
		newClient: newClientFunc,
	}
}

func (cr *Registrar[ID]) GetClient(participantID ID) (Client, error) {
	client, ok := cr.store.load(participantID)
	if !ok {
		var err error
		client, err = cr.newClient(participantID)
		if err != nil {
			return nil, err
		}
		cr.store.add(participantID, client)
	}
	return client, nil
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
