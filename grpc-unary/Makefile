.PHONY: generate

generate:
	go generate ./...

build: generate
	go build -o bin/client ./cmd/client
	go build -o bin/coordinator ./cmd/coordinator