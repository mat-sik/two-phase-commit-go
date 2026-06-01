package client

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
)

func RESTServerRequests(muxes []*http.ServeMux) []RunServerRequest {
	return mapToRunServerRequests(muxes, RunRESTServerRequest)
}

func RunRESTServerRequest(mux *http.ServeMux) RunServerRequest {
	srv := newRESTServer(mux)
	return RunServerRequest{
		serverRunner:  newRESTServerRunner(srv),
		serverStopper: newRESTServerStopper(srv),
	}
}

func newRESTServer(mux *http.ServeMux) *http.Server {
	return &http.Server{
		Handler: mux,
		BaseContext: func(l net.Listener) context.Context {
			return contextWithAddress(context.Background(), l.Addr())
		},
	}
}

func newRESTServerRunner(srv *http.Server) ServerRunner {
	return func(wg *sync.WaitGroup, errCh chan<- error, lis net.Listener) {
		serveFunc := func() error {
			return srv.Serve(lis)
		}
		runServer(wg, errCh, withIgnoreErr(serveFunc, http.ErrServerClosed))
	}
}

func newRESTServerStopper(srv *http.Server) ServerStopper {
	return withIgnoreErr(srv.Close, http.ErrServerClosed)
}

func withIgnoreErr(serverStopper func() error, ignoreErr error) func() error {
	return func() error {
		if err := serverStopper(); err != nil && !errors.Is(err, ignoreErr) {
			return err
		}
		return nil
	}
}
