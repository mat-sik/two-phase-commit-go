package examples

//go:generate protoc --proto_path=./proto --go_out=. --go_opt=module=github.com/mat-sik/two-phase-commit-go/examples --go-grpc_out=. --go-grpc_opt=module=github.com/mat-sik/two-phase-commit-go/examples client.proto
