.PHONY: test test-unit test-integration test-race test-race-stress lint vet build

build:
	go build ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

test-unit:
	go test -race -v ./internal/resourceversion/

test-integration:
	go test -race -v -timeout=180s ./internal/schema/ ./internal/lease/ ./internal/writer/

test-race:
	go test -race -v -count=1 -timeout=120s ./test/race/

test-race-stress:
	go test -race -v -count=100 -timeout=30m ./test/race/

test: test-unit test-integration test-race
