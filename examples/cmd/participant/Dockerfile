FROM golang:1.26.2 AS build-stage

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    go generate ./...

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -o /out/main ./cmd/participant

FROM gcr.io/distroless/static-debian13 AS build-release-stage
COPY --from=build-stage /out/main /main
COPY --from=build-stage /app/db/participant/transfer/migrations ./db/participant/transfer/migrations

USER nonroot:nonroot

EXPOSE 8080

CMD ["/main"]
