KIND_CLUSTER ?= greeting-demo
REPO_ROOT := $(shell cd .. && pwd)

# Use podman if docker is not available.
CONTAINER_TOOL ?= $(shell command -v docker 2>/dev/null || echo podman)
export KIND_EXPERIMENTAL_PROVIDER ?= $(if $(filter podman,$(CONTAINER_TOOL)),podman,)

.PHONY: kind-create kind-delete setup build-etcd build-postgres load-images \
        deploy-etcd deploy-postgres test clean

# --- Cluster lifecycle ---

kind-create:
	kind create cluster --name $(KIND_CLUSTER)
	@echo "Cluster $(KIND_CLUSTER) created"

kind-delete:
	kind delete cluster --name $(KIND_CLUSTER)

# --- Build ---

build-etcd:
	cd etcd-controller && $(CONTAINER_TOOL) build -t greeting-etcd-controller:latest .

build-postgres:
	cd $(REPO_ROOT) && $(CONTAINER_TOOL) build -t greeting-postgres-controller:latest \
		-f examples/postgres-controller/Dockerfile .

build: build-etcd build-postgres

# --- Load images into kind ---

load-images:
	kind load docker-image greeting-etcd-controller:latest --name $(KIND_CLUSTER)
	kind load docker-image greeting-postgres-controller:latest --name $(KIND_CLUSTER)

# --- Deploy ---

deploy-crds:
	kubectl apply -f crd/

deploy-postgres:
	kubectl apply -f deploy/postgres.yaml
	kubectl wait --for=condition=available deployment/postgres --timeout=120s

deploy-etcd: deploy-crds
	kubectl apply -f deploy/etcd-controller.yaml
	kubectl wait --for=condition=available deployment/greeting-etcd-controller --timeout=60s

deploy-postgres-ctrl: deploy-postgres
	kubectl apply -f deploy/postgres-controller.yaml
	kubectl wait --for=condition=available deployment/greeting-postgres-controller --timeout=60s

# --- Full setup ---

setup: kind-create build load-images deploy-crds deploy-postgres deploy-etcd deploy-postgres-ctrl
	@echo ""
	@echo "=== Setup complete ==="
	@echo "etcd controller:    kubectl logs -f deployment/greeting-etcd-controller"
	@echo "postgres controller: kubectl logs -f deployment/greeting-postgres-controller"

# --- Test ---

test:
	bash scripts/test.sh

# --- Cleanup ---

clean: kind-delete
