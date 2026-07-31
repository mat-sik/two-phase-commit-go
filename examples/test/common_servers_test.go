package test

import (
	"net"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/two-phase-commit-go/examples/internal/participant/adapter"
)

func runServers(launch []serverLaunch) (serverBundle, error) {
	listeners := make([]net.Listener, 0, len(launch))
	for range launch {
		lis, err := net.Listen("tcp", ":0")
		if err != nil {
			return serverBundle{}, err
		}
		listeners = append(listeners, lis)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(launch))

	serversErrCh := make(chan error, len(launch))

	serverHandles := make([]serverHandle, 0, len(launch))
	for i, lis := range listeners {
		go launch[i].serverRunner(&wg, serversErrCh, lis)

		serverHandles = append(serverHandles, serverHandle{
			address:       localhostAddress(lis),
			serverStopper: launch[i].serverStopper,
		})
	}

	go func() {
		wg.Wait()
		close(serversErrCh)
	}()

	return serverBundle{
		serverHandles: serverHandles,
		serversErrCh:  serversErrCh,
	}, nil
}

func localhostAddress(lis net.Listener) string {
	address := lis.Addr().String()
	_, port, _ := net.SplitHostPort(address)
	return "localhost:" + port
}

type serverLaunch struct {
	serverRunner  serverRunner
	serverStopper serverStopper
}

var noopServerLaunch = serverLaunch{
	serverRunner:  noopServerRunner,
	serverStopper: noopServerStopper,
}

type serverSpec interface {
	toServerLaunch() serverLaunch
}

type noopServerSpec struct {
}

func (n noopServerSpec) toServerLaunch() serverLaunch {
	return noopServerLaunch
}

type restBasicLogicServerSpec struct {
	prepareFailUntilAttempt, commitFailUntilAttempt, rollbackFailUntilAttempt int
}

func (r restBasicLogicServerSpec) toServerLaunch() serverLaunch {
	return mapFromMux(adapter.NewFailingBasicMux(r.prepareFailUntilAttempt, r.commitFailUntilAttempt, r.rollbackFailUntilAttempt))
}

type gRPCBasicLogicServerSpec struct {
	prepareFailUntilAttempt, commitFailUntilAttempt, rollbackFailUntilAttempt int
}

func (r gRPCBasicLogicServerSpec) toServerLaunch() serverLaunch {
	return mapFromGRPCBasicHandler(adapter.NewFailingBasicGRPCHandler(r.prepareFailUntilAttempt, r.commitFailUntilAttempt, r.rollbackFailUntilAttempt))
}

type serverSpecWithPool interface {
	serverSpec
	injectPool(pool *pgxpool.Pool)
}

type restTransferLogicServerSpec struct {
	pool *pgxpool.Pool
}

func (r *restTransferLogicServerSpec) toServerLaunch() serverLaunch {
	return mapFromMux(adapter.NewTransferMux(r.pool))
}

func (r *restTransferLogicServerSpec) injectPool(pool *pgxpool.Pool) {
	r.pool = pool
}

type gRPCTransferLogicServerSpec struct {
	pool *pgxpool.Pool
}

func (r *gRPCTransferLogicServerSpec) toServerLaunch() serverLaunch {
	return mapFromGRPCTransferHandler(adapter.NewTransferGRPCHandler(r.pool))
}

func (r *gRPCTransferLogicServerSpec) injectPool(pool *pgxpool.Pool) {
	r.pool = pool
}

type serverBundle struct {
	serverHandles []serverHandle
	serversErrCh  <-chan error
}

type serverHandle struct {
	address       string
	serverStopper serverStopper
}

func (sb serverBundle) addresses() []string {
	addresses := make([]string, 0, len(sb.serverHandles))
	for _, srv := range sb.serverHandles {
		addresses = append(addresses, srv.address)
	}
	return addresses
}

func (sb serverBundle) shutdown() []error {
	wg := sync.WaitGroup{}
	serverStoppers := sb.serverStoppers()
	errCh := make(chan error, len(serverStoppers))
	for _, srvStopper := range serverStoppers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := srvStopper()
			if err != nil {
				errCh <- err
			}
		}()
	}
	go func() {
		wg.Wait()
		close(errCh)
	}()

	var errs []error
	for err := range sb.serversErrCh {
		if err != nil {
			errs = append(errs, err)
		}
	}
	for err := range errCh {
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (sb serverBundle) serverStoppers() []serverStopper {
	serverStoppers := make([]serverStopper, 0, len(sb.serverHandles))
	for _, srv := range sb.serverHandles {
		serverStoppers = append(serverStoppers, srv.serverStopper)
	}
	return serverStoppers
}

type serverRunner func(wg *sync.WaitGroup, errCh chan<- error, lis net.Listener)

var noopServerRunner = func(wg *sync.WaitGroup, errCh chan<- error, lis net.Listener) {
	defer wg.Done()
	errCh <- lis.Close()
}

type serverStopper func() error

var noopServerStopper = func() error {
	return nil
}

func runServer(wg *sync.WaitGroup, errCh chan<- error, serveFunc func() error) {
	defer wg.Done()
	if err := serveFunc(); err != nil {
		errCh <- err
	}
}

func mapToServerLaunches[T any](elements []T, mappingFunc func(T) serverLaunch) []serverLaunch {
	serverLaunches := make([]serverLaunch, 0, len(elements))
	for _, el := range elements {
		serverLaunches = append(serverLaunches, mappingFunc(el))
	}
	return serverLaunches
}
