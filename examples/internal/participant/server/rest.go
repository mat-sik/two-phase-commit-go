package server

import (
	"context"
	"net"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func NewRESTServer(mux *http.ServeMux) *http.Server {
	return &http.Server{
		Handler: otelhttp.NewHandler(mux, "participant-http"),
		BaseContext: func(l net.Listener) context.Context {
			return contextWithAddress(context.Background(), l.Addr())
		},
	}
}
