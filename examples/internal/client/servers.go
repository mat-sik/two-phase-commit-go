package client

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"sync"
)

func RunServers(requests []RunServerRequest) (ServerBundle, error) {
	listeners := make([]net.Listener, 0, len(requests))
	for range requests {
		lis, err := net.Listen("tcp", ":0")
		if err != nil {
			return ServerBundle{}, err
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

	return ServerBundle{
		serverHandles: serverHandles,
		serversErrCh:  serversErrCh,
	}, nil
}

func localhostAddress(lis net.Listener) string {
	address := lis.Addr().String()
	_, port, _ := net.SplitHostPort(address)
	return "localhost:" + port
}

type RunServerRequest struct {
	serverRunner  ServerRunner
	serverStopper ServerStopper
	addr          *string
}

func (rsr RunServerRequest) getAddr() string {
	if rsr.addr != nil {
		return *rsr.addr
	}
	return ":0"
}

type ServerBundle struct {
	serverHandles []serverHandle
	serversErrCh  <-chan error
}

type serverHandle struct {
	address       string
	serverStopper ServerStopper
}

func (sb ServerBundle) Addresses() []string {
	addresses := make([]string, 0, len(sb.serverHandles))
	for _, srv := range sb.serverHandles {
		addresses = append(addresses, srv.address)
	}
	return addresses
}

func (sb ServerBundle) Shutdown() []error {
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

func (sb ServerBundle) serverStoppers() []ServerStopper {
	serverStoppers := make([]ServerStopper, 0, len(sb.serverHandles))
	for _, srv := range sb.serverHandles {
		serverStoppers = append(serverStoppers, srv.serverStopper)
	}
	return serverStoppers
}

type ServerRunner func(wg *sync.WaitGroup, errCh chan<- error, lis net.Listener)

type ServerStopper func() error

func runServer(wg *sync.WaitGroup, errCh chan<- error, serveFunc func() error) {
	defer wg.Done()
	if err := serveFunc(); err != nil {
		errCh <- err
	}
}

func mapToRunServerRequests[T any](elements []T, mappingFunc func(T) RunServerRequest) []RunServerRequest {
	runServerRequests := make([]RunServerRequest, 0, len(elements))
	for _, el := range elements {
		runServerRequests = append(runServerRequests, mappingFunc(el))
	}
	return runServerRequests
}

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
