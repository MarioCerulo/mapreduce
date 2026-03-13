run:
    @go run main.go

test:
    @go test ./...

proto:
    @protoc --proto_path=engine/rpc/proto \
       --go_out=engine/rpc --go_opt=paths=source_relative \
       --go-grpc_out=engine/rpc --go-grpc_opt=paths=source_relative \
       coordinator.proto
