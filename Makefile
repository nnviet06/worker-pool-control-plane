.PHONY: vet test proto

vet:
	go vet ./...

test:
	go test -race ./...

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/proto/worker.proto
