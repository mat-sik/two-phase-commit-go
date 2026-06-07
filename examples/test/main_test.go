package test

import (
	"testing"

	"github.com/mat-sik/two-phase-commit-go/examples/internal/client"
)

func TestMain(m *testing.M) {
	client.InitLogger()
	m.Run()
}
