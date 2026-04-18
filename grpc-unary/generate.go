package grpc_unary

//go:generate protoc --proto_path=./proto --go_out=. --go_opt=module=github.com/mat-sik/two-phase-commit-go/grpc-unary --go-grpc_out=. --go-grpc_opt=module=github.com/mat-sik/two-phase-commit-go/grpc-unary client.proto
