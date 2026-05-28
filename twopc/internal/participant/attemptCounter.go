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

func (fc *AttemptCounter[ID]) Fail(participantID ID) {
	v, _ := fc.store.LoadOrStore(participantID, &atomic.Int64{})
	v.(*atomic.Int64).Add(1)
}

func (fc *AttemptCounter[ID]) Success(participantID ID) {
	if v, ok := fc.store.Load(participantID); ok {
		v.(*atomic.Int64).Store(0)
	}
}

func (fc *AttemptCounter[ID]) Attempt(participantID ID) int {
	if v, ok := fc.store.Load(participantID); ok {
		return int(v.(*atomic.Int64).Load())
	}
	return 0
}
