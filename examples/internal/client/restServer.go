package client

import (
	"context"
	"net"
	"net/http"
)

func NewRESTServer(mux *http.ServeMux) *http.Server {
	return &http.Server{
		Handler: mux,
		BaseContext: func(l net.Listener) context.Context {
			return contextWithAddress(context.Background(), l.Addr())
		},
	}
}
