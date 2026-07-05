# syntax=docker/dockerfile:1.4

FROM golang:1.26.2 AS build-stage

WORKDIR /app

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GOCACHE=/root/.cache/go-build \
    GOMODCACHE=/go/pkg/mod

RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
    apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ARG PROTOC_GEN_GO_VERSION=v1.36.11
ARG PROTOC_GEN_GO_GRPC_VERSION=v1.6.2

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION} && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}

COPY go.mod go.sum ./

COPY proto ./proto
COPY generate.go ./
RUN go generate ./...

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o /out/main ./cmd/coordinator

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build-stage /out/main /main
COPY --from=build-stage /app/db/coordinator/migrations ./db/coordinator/migrations

EXPOSE 8080

ENTRYPOINT ["/main"]
