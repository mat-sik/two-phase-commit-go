package test

import (
	"errors"
	"net"
	"net/http"
	"sync"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/server"
)

func restServerRequests(muxes []*http.ServeMux) []runServerRequest {
	return mapToRunServerRequests(muxes, mapFromMux)
}

func mapFromMux(mux *http.ServeMux) runServerRequest {
	srv := server.NewRESTServer(mux)
	return runServerRequest{
		serverRunner:  newRESTServerRunner(srv),
		serverStopper: newRESTServerStopper(srv),
	}
}

func newRESTServerRunner(srv *http.Server) serverRunner {
	return func(wg *sync.WaitGroup, errCh chan<- error, lis net.Listener) {
		serveFunc := func() error {
			return srv.Serve(lis)
		}
		runServer(wg, errCh, withIgnoreErr(serveFunc, http.ErrServerClosed))
	}
}

func newRESTServerStopper(srv *http.Server) serverStopper {
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
