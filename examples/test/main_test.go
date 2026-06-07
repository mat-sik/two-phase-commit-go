package test

import (
	"testing"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/client/server"
)

func TestMain(m *testing.M) {
	server.InitLogger()
	m.Run()
}
