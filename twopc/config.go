package twopc

import "time"

type config struct {
	sendOperationTimeout time.Duration
	backoffBase          time.Duration
	backoffMax           time.Duration
	backoffFactor        float64
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
