package config

import (
	"context"

	"github.com/sethvargo/go-envconfig"
)

type Coordinator struct {
	Port        int    `env:"PORT, default=8080"`
	DatabaseURL string `env:"DATABASE_URL"`
}

func NewCoordinator(ctx context.Context) (Collector, error) {
	var config Collector
	if err := envconfig.Process(ctx, &config); err != nil {
		return Collector{}, err
	}

	return config, nil
}
