package client

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
)

type netAddrKey struct{}

func contextWithAddress(ctx context.Context, addr net.Addr) context.Context {
	return context.WithValue(ctx, netAddrKey{}, addr)
}

func addressFromContext(ctx context.Context) (net.Addr, error) {
	addr, ok := ctx.Value(netAddrKey{}).(net.Addr)
	if !ok {
		return nil, errors.New("could not get net.Addr from context")
	}
	return addr, nil
}

func InitLogger() {
	slog.SetDefault(slog.New(&netAddrSlogHandler{
		Handler: slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}),
	}))
}

type netAddrSlogHandler struct {
	slog.Handler
}

func (h *netAddrSlogHandler) Handle(ctx context.Context, r slog.Record) error {
	addr, err := addressFromContext(ctx)
	if err == nil {
		r.AddAttrs(slog.String("netAddr", addr.String()))
	}
	return h.Handler.Handle(ctx, r)
}
