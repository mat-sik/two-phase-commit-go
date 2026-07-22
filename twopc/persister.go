package twopc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/mat-sik/two-phase-commit-go/twopc/internal/transaction"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type persister[ID comparable] struct {
	rootCtx                   context.Context
	rootCancel                context.CancelFunc
	wg                        sync.WaitGroup
	handlerStore              *persisterHandlerStore[ID]
	errorsStore               *persisterErrorStore
	transactionStatePersister transactionStatePersister[ID]
	tracer                    trace.Tracer
}

func newPersister[ID comparable](
	ctx context.Context,
	transactionStatePersister transactionStatePersister[ID],
	tracer trace.Tracer,
) *persister[ID] {
	ctx, cancel := context.WithCancel(ctx)
	return &persister[ID]{
		rootCtx:                   ctx,
		rootCancel:                cancel,
		handlerStore:              &persisterHandlerStore[ID]{},
		errorsStore:               &persisterErrorStore{},
		transactionStatePersister: transactionStatePersister,
		tracer:                    tracer,
	}
}

func (p *persister[ID]) enqueuePersistState(ctx context.Context, txID string, participantID ID, state transaction.State) {
	if p.rootCtx.Err() != nil {
		return
	}
	p.wg.Go(func() {
		p.persistState(ctx, txID, participantID, state)
	})
}

func (p *persister[ID]) persistState(ctx context.Context, txID string, participantID ID, state transaction.State) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(p.rootCtx, cancel)

	var span trace.Span
	ctx, span = persistStateSpan(ctx, p.tracer, txID, participantID, state)

	done := make(chan struct{})

	key := persisterHandlerKey[ID]{
		txID:          txID,
		participantID: participantID,
	}
	handle := &persisterHandle[ID]{
		done: done,
	}

	defer func() {
		stop()
		cancel()
		close(done)
		p.handlerStore.compareAndDelete(key, handle)
		span.End()
	}()

	if prev, ok := p.handlerStore.swap(key, handle); ok {
		select {
		case <-ctx.Done():
		case <-prev.done:
		}
	}

	if ctx.Err() == nil {
		if err := p.transactionStatePersister.PersistState(ctx, txID, participantID, state); err != nil {
			p.errorsStore.add(err)
			span.RecordError(err)
			span.SetStatus(codes.Error, "persist state")
		}
	}
}

func persistStateSpan[ID comparable](
	ctx context.Context,
	tracer trace.Tracer,
	txID string,
	participantID ID,
	state transaction.State,
) (context.Context, trace.Span) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "persist-state")

	span.SetAttributes(
		attribute.String("transaction.id", txID),
		attribute.String("participant.id", fmt.Sprintf("%v", participantID)),
		attribute.Int("state", int(state)),
	)

	return ctx, span
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
