package participant

import (
	"sync"
	"sync/atomic"
)

type AttemptCounter[ID comparable] struct {
	store sync.Map
}

func NewFailureCounter[ID comparable]() *AttemptCounter[ID] {
	return &AttemptCounter[ID]{
		store: sync.Map{},
	}
}

func (ac *AttemptCounter[ID]) Fail(participantID ID) {
	v, _ := ac.store.LoadOrStore(participantID, &atomic.Int64{})
	v.(*atomic.Int64).Add(1)
}

func (ac *AttemptCounter[ID]) Success(participantID ID) {
	if v, ok := ac.store.Load(participantID); ok {
		v.(*atomic.Int64).Store(0)
	}
}

func (ac *AttemptCounter[ID]) Attempt(participantID ID) int {
	if v, ok := ac.store.Load(participantID); ok {
		return int(v.(*atomic.Int64).Load())
	}
	return 0
}
