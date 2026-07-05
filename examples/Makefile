.PHONY: generate test test-no-testcontainers test-with-logs test-no-testcontainers-with-logs docker-build-coordinator docker-build-participant docker-build

generate:
	go generate ./...

test:
	go test -tags testcontainers ./...

test-no-testcontainers:
	go test ./...

test-with-logs:
	go test -tags testcontainers -v ./...

test-no-testcontainers-with-logs:
	go test -v ./...

VERSION ?= $(shell git describe --tags --abbrev=0 --match 'twopc/v*' | sed 's#twopc/v##')

docker-build-coordinator:
	docker build -f cmd/coordinator/Dockerfile -t coordinator:$(VERSION) .

docker-build-participant:
	docker build -f cmd/participant/Dockerfile -t participant:$(VERSION) .

docker-build: docker-build-coordinator docker-build-participant