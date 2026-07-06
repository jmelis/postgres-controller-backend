.PHONY: test test-unit test-integration test-race test-race-stress test-toxirace test-parity lint vet build

build:
	go build ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

test-unit:
	go test -race -v ./internal/resourceversion/

test-integration:
	go test -race -v -timeout=180s ./internal/schema/ ./internal/lease/ ./internal/writer/ ./internal/reader/ ./internal/compaction/ ./internal/verifier/

test-race:
	go test -race -v -count=1 -timeout=120s ./test/race/

test-race-stress:
	go test -race -v -count=100 -timeout=30m ./test/race/

test-toxirace:
	go test -race -v -count=1 -timeout=180s ./test/toxirace/

test-load:
	go test -race -v -count=1 -timeout=120s ./test/loadtest/

test-parity:
	@which setup-envtest > /dev/null 2>&1 || go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
	KUBEBUILDER_ASSETS=$$(setup-envtest use --bin-dir $$(pwd)/.envtest -p path) \
	go test -race -v -count=1 -timeout=180s ./test/parity/

test: test-unit test-integration test-race test-parity
