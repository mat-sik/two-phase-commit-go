package retry

import (
	"context"
	"math"
	"math/rand/v2"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Backoff struct {
	base   time.Duration
	max    time.Duration
	factor float64
}

func NewBackoff(base time.Duration, max time.Duration, factor float64) Backoff {
	if base == 0 || max == 0 || factor <= 1 {
		panic("base, max, and factor must be positive")
	}
	return Backoff{
		base:   base,
		max:    max,
		factor: factor,
	}
}

func (s Backoff) Wait(ctx context.Context, attempt int) {
	delay := nextSleepDuration(s.base, s.max, s.factor, attempt)

	span := trace.SpanFromContext(ctx)
	span.AddEvent("backoff waiting",
		trace.WithAttributes(
			attribute.Int("backoff.attempt", attempt),
			attribute.Int64("backoff.wait.delay.ms", delay.Milliseconds()),
		),
	)

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func nextSleepDuration(base time.Duration, max time.Duration, factor float64, attempt int) time.Duration {
	attemptFactor := math.Pow(factor, float64(attempt))
	if math.IsInf(attemptFactor, 1) || attemptFactor > float64(max/base) {
		return rand.N(max)
	}
	delay := base * time.Duration(attemptFactor)
	return rand.N(delay)
}
