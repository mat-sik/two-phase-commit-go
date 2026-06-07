package test

import (
	"net"
	"sync"
)

func runServers(requests []runServerRequest) (serverBundle, error) {
	listeners := make([]net.Listener, 0, len(requests))
	for range requests {
		lis, err := net.Listen("tcp", ":0")
		if err != nil {
			return serverBundle{}, err
		}
		listeners = append(listeners, lis)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(requests))

	serversErrCh := make(chan error, len(requests))

	serverHandles := make([]serverHandle, 0, len(requests))
	for i, lis := range listeners {
		go requests[i].serverRunner(&wg, serversErrCh, lis)

		serverHandles = append(serverHandles, serverHandle{
			address:       localhostAddress(lis),
			serverStopper: requests[i].serverStopper,
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

type runServerRequest struct {
	serverRunner  serverRunner
	serverStopper serverStopper
	addr          *string
}

func (rsr runServerRequest) getAddr() string {
	if rsr.addr != nil {
		return *rsr.addr
	}
	return ":0"
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
	for _, serverStopper := range serverStoppers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := serverStopper()
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

type serverStopper func() error

func runServer(wg *sync.WaitGroup, errCh chan<- error, serveFunc func() error) {
	defer wg.Done()
	if err := serveFunc(); err != nil {
		errCh <- err
	}
}

func mapToRunServerRequests[T any](elements []T, mappingFunc func(T) runServerRequest) []runServerRequest {
	runServerRequests := make([]runServerRequest, 0, len(elements))
	for _, el := range elements {
		runServerRequests = append(runServerRequests, mappingFunc(el))
	}
	return runServerRequests
}
