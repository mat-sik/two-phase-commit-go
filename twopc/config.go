package twopc

import (
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type config struct {
	sendOperationTimeout time.Duration
	backoffBase          time.Duration
	backoffMax           time.Duration
	backoffFactor        float64
	tracer               trace.Tracer
}

func newConfig(opts ...Option) config {
	cfg := newDefaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func newDefaultConfig() config {
	return config{
		sendOperationTimeout: 5 * time.Second,
		backoffBase:          200 * time.Millisecond,
		backoffMax:           10 * time.Second,
		backoffFactor:        2,
		tracer:               noop.NewTracerProvider().Tracer("noop"),
	}
}

// Option configures a [Coordinator] created by [NewCoordinator].
type Option func(*config)

// WithSendOperationTimeout sets the per-attempt timeout for sending a single
// operation (Prepare, Commit, or Rollback) to a participant.
// Defaults to 5s.
func WithSendOperationTimeout(d time.Duration) Option {
	return func(c *config) { c.sendOperationTimeout = d }
}

// WithBackoffBase sets the initial backoff delay before retrying a participant
// that previously failed. Defaults to 200ms.
func WithBackoffBase(d time.Duration) Option {
	return func(c *config) { c.backoffBase = d }
}

// WithBackoffMax sets the maximum backoff delay between retries.
// Defaults to 10s.
func WithBackoffMax(d time.Duration) Option {
	return func(c *config) { c.backoffMax = d }
}

// WithBackoffFactor sets the exponential growth factor applied to the backoff
// delay after each failed attempt. Must be greater than 1. Defaults to 2.
func WithBackoffFactor(f float64) Option {
	return func(c *config) { c.backoffFactor = f }
}

// WithOTelTracer sets the OpenTelemetry tracer used to instrument coordinator
// execution. When set, the coordinator emits spans for the full transaction
// lifecycle: the outer transaction loop, each execution round, per-participant
// operations, backoff waits, and async state persistence.
//
// Defaults to a noop tracer, which produces no telemetry and adds no overhead.
//
// Span attributes include participant IDs and prepare payloads formatted via
// [fmt.Sprintf] with the %v verb. To produce human-readable values in spans,
// implement [fmt.Stringer] on the ID type and [PreparePayload] concrete type.
func WithOTelTracer(tracer trace.Tracer) Option {
	return func(c *config) {
		c.tracer = tracer
	}
}
