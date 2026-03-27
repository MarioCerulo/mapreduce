coordinator reducers='1':
    @go run cmd/coordinator/main.go --reducers={{reducers}}

worker:
    @go run cmd/worker/main.go

test:
    @go test -v ./...

proto:
    @protoc --proto_path=rpc/proto \
       --go_out=rpc --go_opt=paths=source_relative \
       --go-grpc_out=rpc --go-grpc_opt=paths=source_relative \
       coordinator.proto
