package test

import (
	"context"
	"testing"
	"time"

	"github.com/mat-sik/two-phase-commit-go/twopc"
)

type testCase struct {
	name          string
	serverConfigs []serverConfig
	txCoordinator *twopc.Coordinator[string]
	request       twopc.DistributedTransaction[string]
	wantErr       bool
	wantedOutcome twopc.Outcome
}

func runTest(t *testing.T, tt testCase) {
	t.Helper()

	srvBundle, err := runServers(tt.serverConfigs)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	outcome := tt.txCoordinator.Execute(ctx, tt.request)
	if tt.wantedOutcome != outcome.Outcome() {
		t.Fatalf("expected outcome %v, got %v", tt.wantedOutcome, outcome.Outcome())
	}
	if tt.wantErr && outcome.Err() == nil {
		t.Fatalf("expected error")
	}
	if !tt.wantErr && outcome.Err() != nil {
		t.Fatalf("didn't expect error, got %v", outcome.Err())
	}

	for _, server := range srvBundle.servers {
		go server.GracefulStop()
	}

	var errs []error
	for err = range srvBundle.serverErrsChan {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		t.Errorf("got %d server errors: %v", len(errs), errs)
	}
}
