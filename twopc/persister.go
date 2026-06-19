package twopc

import (
	"context"
	"errors"
	"sync"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

type persister[ID comparable] struct {
	rootCtx                   context.Context
	rootCancel                context.CancelFunc
	wg                        sync.WaitGroup
	handlerStore              *persisterHandlerStore[ID]
	errorsStore               *persisterErrorStore
	transactionStatePersister transactionStatePersister[ID]
}

func newPersister[ID comparable](ctx context.Context, transactionStatePersister transactionStatePersister[ID]) *persister[ID] {
	ctx, cancel := context.WithCancel(ctx)
	return &persister[ID]{
		rootCtx:                   ctx,
		rootCancel:                cancel,
		handlerStore:              &persisterHandlerStore[ID]{},
		errorsStore:               &persisterErrorStore{},
		transactionStatePersister: transactionStatePersister,
	}
}

func (p *persister[ID]) enqueuePersistState(ctx context.Context, txID string, participantID ID, state transaction.State) {
	if p.rootCtx.Err() != nil {
		return
	}
	p.wg.Add(1)
	go p.persistState(ctx, txID, participantID, state)
}

func (p *persister[ID]) persistState(ctx context.Context, txID string, participantID ID, state transaction.State) {
	defer p.wg.Done()

	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(p.rootCtx, cancel)
	defer stop()

	done := make(chan struct{})

	key := persisterHandlerKey[ID]{
		txID:          txID,
		participantID: participantID,
	}
	handle := &persisterHandle[ID]{
		done: done,
	}

	defer func() {
		cancel()
		close(done)
		p.handlerStore.compareAndDelete(key, handle)
	}()

	if prev, ok := p.handlerStore.swap(key, handle); ok {
		select {
		case <-ctx.Done():
		case <-prev.done:
		}
	}

	if ctx.Err() == nil {
		err := p.transactionStatePersister.PersistState(ctx, txID, participantID, state)
		p.errorsStore.add(err)
	}
}

func (p *persister[ID]) stop() error {
	p.rootCancel()
	p.wg.Wait()
	return p.errorsStore.errorsJoin()
}

type persisterHandlerStore[ID comparable] struct {
	store sync.Map
}

func (s *persisterHandlerStore[ID]) swap(key persisterHandlerKey[ID], value *persisterHandle[ID]) (*persisterHandle[ID], bool) {
	prev, ok := s.store.Swap(key, value)
	if !ok {
		return nil, false
	}
	return prev.(*persisterHandle[ID]), true
}

func (s *persisterHandlerStore[ID]) compareAndDelete(key persisterHandlerKey[ID], value *persisterHandle[ID]) bool {
	return s.store.CompareAndDelete(key, value)
}

type persisterErrorStore struct {
	mu     sync.Mutex
	errors []error
}

func (es *persisterErrorStore) add(err error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.errors = append(es.errors, err)
}

func (es *persisterErrorStore) errorsJoin() error {
	es.mu.Lock()
	defer es.mu.Unlock()
	return errors.Join(es.errors...)
}

type persisterHandlerKey[ID comparable] struct {
	txID          string
	participantID ID
}

type persisterHandle[ID comparable] struct {
	done <-chan struct{}
}
