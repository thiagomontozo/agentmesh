.PHONY: run test vet fmt build

run:
	go run ./cmd/agentmesh

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w ./cmd ./internal

build:
	go build -o bin/agentmesh ./cmd/agentmesh
