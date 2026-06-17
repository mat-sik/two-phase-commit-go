package twopc

import (
	"context"
	"sync"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
)

type persister[ID comparable] struct {
	rootCtx                   context.Context
	rootCancel                context.CancelFunc
	wg                        sync.WaitGroup
	persisterHandlerStore     *persisterHandlerStore[ID]
	transactionStatePersister transactionStatePersister[ID]
}

func newPersister[ID comparable](ctx context.Context, transactionStatePersister transactionStatePersister[ID]) *persister[ID] {
	ctx, cancel := context.WithCancel(ctx)
	return &persister[ID]{
		rootCtx:                   ctx,
		rootCancel:                cancel,
		persisterHandlerStore:     &persisterHandlerStore[ID]{},
		transactionStatePersister: transactionStatePersister,
	}
}

func (p *persister[ID]) enqueuePersistState(ctx context.Context, txID string, participantID ID, state transaction.State) {
	if p.rootCtx.Err() != nil {
		return
	}

	p.wg.Add(1)
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
		p.persisterHandlerStore.compareAndDelete(key, handle)
	}()

	prev, ok := p.persisterHandlerStore.swap(key, handle)
	if ok {
		select {
		case <-ctx.Done():
		case <-prev.done:
		}
	}

	if ctx.Err() == nil {
		p.transactionStatePersister.PersistState(ctx, txID, participantID, state)
	}
}

func (p *persister[ID]) stop() {
	p.rootCancel()
	p.wg.Wait()
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

type persisterHandlerKey[ID comparable] struct {
	txID          string
	participantID ID
}

type persisterHandle[ID comparable] struct {
	done <-chan struct{}
}
