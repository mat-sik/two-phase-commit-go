package config

import (
	"context"

	"github.com/sethvargo/go-envconfig"
)

type Collector struct {
	CollectorHost string `env:"OTEL_COLLECTOR_HOST, default=localhost:4317"`
	ServiceName   string `env:"OTEL_SERVICE_NAME, default=unnamed"`
}

func NewCollector(ctx context.Context) (Collector, error) {
	var config Collector
	if err := envconfig.Process(ctx, &config); err != nil {
		return Collector{}, err
	}

	return config, nil
}
