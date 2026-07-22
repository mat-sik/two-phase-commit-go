package config

import (
	"context"
	"fmt"

	"github.com/sethvargo/go-envconfig"
)

type Protocol string

const (
	ProtocolREST Protocol = "REST"
	ProtocolGRPC Protocol = "GRPC"
)

type Mode string

const (
	ModeTransfer Mode = "TRANSFER"
	ModeBasic    Mode = "BASIC"
)

type Participant struct {
	Port        int      `env:"PORT, default=8080"`
	Protocol    Protocol `env:"PROTOCOL, default=GRPC"`
	Mode        Mode     `env:"MODE, default=BASIC"`
	DatabaseURL string   `env:"DATABASE_URL"`
}

func (p Participant) Validate() error {
	switch p.Protocol {
	case ProtocolREST, ProtocolGRPC:
	default:
		return fmt.Errorf("unknown protocol %q: must be REST or GRPC", p.Protocol)
	}

	switch p.Mode {
	case ModeTransfer, ModeBasic:
	default:
		return fmt.Errorf("unknown mode %q: must be TRANSFER or BASIC", p.Mode)
	}

	if p.ShouldInitDBPool() && p.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required when MODE is TRANSFER")
	}
	return nil
}

func (p Participant) ShouldInitDBPool() bool {
	return p.Mode == ModeTransfer
}

func NewParticipant(ctx context.Context) (Participant, error) {
	var config Participant
	if err := envconfig.Process(ctx, &config); err != nil {
		return Participant{}, fmt.Errorf("processing participant env variables")
	}
	if err := config.Validate(); err != nil {
		return Participant{}, err
	}
	return config, nil
}
