package participant

import (
	"context"
	"fmt"
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

func NewRegistrar[ID comparable](
	newClientFunc func(participantID ID) (Client, error),
	clients map[ID]Client,
) Registrar[ID] {
	return Registrar[ID]{
		store:     newRegistrarStore(clients),
		newClient: newClientFunc,
	}
}

var ErrInvalidClientConfig = fmt.Errorf("invalid coordinator client configuration")

func (r *Registrar[ID]) GetClient(participantID ID) (Client, error) {
	client, ok := r.store.load(participantID)
	if !ok && r.newClient == nil {
		return nil, fmt.Errorf(
			"%w: newClientFunc is not provided and clients map doesn't contain participantID: %v",
			ErrInvalidClientConfig,
			participantID,
		)
	}

	if !ok {
		var err error
		client, err = r.newClient(participantID)
		if err != nil {
			return nil, err
		}
		r.store.add(participantID, client)
	}
	return client, nil
}

type registrarStore[ID comparable] struct {
	store sync.Map
}

func newRegistrarStore[ID comparable](clients map[ID]Client) *registrarStore[ID] {
	store := registrarStore[ID]{}
	for id, client := range clients {
		store.add(id, client)
	}
	return &store
}

func (rs *registrarStore[ID]) add(id ID, client Client) {
	rs.store.Store(id, client)
}

func (rs *registrarStore[ID]) load(id ID) (Client, bool) {
	value, ok := rs.store.Load(id)
	if !ok {
		return nil, false
	}
	return value.(Client), true
}
