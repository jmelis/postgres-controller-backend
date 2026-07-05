#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPEC_FILE="${1:-specs/5k-baseline.yaml}"
NAMESPACE="pgctl-loadtest"
IMAGE_TAG="${IMAGE_TAG:-latest}"
IMAGE_NAME="${IMAGE_NAME:-pgctl-loadtest}"

usage() {
    cat <<EOF
Usage: $0 [SPEC_FILE] [COMMAND]

Commands:
  setup       Provision infrastructure (Terraform + K8s manifests)
  run         Build, push, and run the load test Job
  check       Show current checkpoint (mid-run progress)
  status      Show Job status and pod info
  results     Fetch final results from the completed Job
  teardown    Destroy everything (Terraform + K8s resources)
  all         Run setup + run (default if no command given)

Arguments:
  SPEC_FILE   Path to the YAML test spec (default: specs/5k-baseline.yaml)

Environment:
  IMAGE_NAME  Container image name (default: pgctl-loadtest)
  IMAGE_TAG   Container image tag (default: latest)

Examples:
  $0 specs/5k-baseline.yaml all
  $0 specs/ceiling-hunt.yaml run
  $0 check
  $0 teardown
EOF
}

log() { echo "==> $*"; }

terraform_apply() {
    log "Provisioning infrastructure with Terraform..."
    cd "$SCRIPT_DIR/terraform"
    terraform init -upgrade
    terraform apply -auto-approve
    cd "$SCRIPT_DIR"
}

configure_kubectl() {
    log "Configuring kubectl..."
    cd "$SCRIPT_DIR/terraform"
    eval "$(terraform output -raw kubeconfig_update_command)"
    cd "$SCRIPT_DIR"
}

apply_k8s_manifests() {
    log "Applying K8s manifests..."
    kubectl apply -f "$SCRIPT_DIR/k8s/namespace.yaml"
    kubectl apply -f "$SCRIPT_DIR/k8s/serviceaccount.yaml"
    kubectl apply -f "$SCRIPT_DIR/k8s/cloudwatch-agent-config.yaml"

    # Check if RDS secret exists
    if ! kubectl get secret rds-dsn -n "$NAMESPACE" &>/dev/null; then
        log "Creating RDS secret from Terraform output..."
        cd "$SCRIPT_DIR/terraform"
        DSN=$(terraform output -raw rds_connection_string)
        kubectl create secret generic rds-dsn \
            --from-literal=dsn="$DSN" \
            -n "$NAMESPACE"
        cd "$SCRIPT_DIR"
    fi

    # Create ConfigMap from spec file
    log "Loading spec: $SPEC_FILE"
    kubectl create configmap loadtest-spec \
        --from-file=spec.yaml="$SCRIPT_DIR/$SPEC_FILE" \
        -n "$NAMESPACE" \
        -o yaml --dry-run=client | kubectl apply -f -
}

build_and_push() {
    log "Building load test image..."
    cd "$SCRIPT_DIR/.."
    podman build -t "$IMAGE_NAME:$IMAGE_TAG" -f loadtest/Containerfile .

    # If using ECR, push; otherwise assume local/kind
    if [[ -n "${ECR_REGISTRY:-}" ]]; then
        log "Pushing to ECR..."
        podman tag "$IMAGE_NAME:$IMAGE_TAG" "$ECR_REGISTRY/$IMAGE_NAME:$IMAGE_TAG"
        podman push "$ECR_REGISTRY/$IMAGE_NAME:$IMAGE_TAG"
    else
        log "No ECR_REGISTRY set — assuming image is available to the cluster"
    fi
    cd "$SCRIPT_DIR"
}

run_job() {
    log "Starting load test Job..."
    # Delete previous Job if it exists
    kubectl delete job pgctl-loadtest -n "$NAMESPACE" --ignore-not-found

    # Apply the Job manifest
    kubectl apply -f "$SCRIPT_DIR/k8s/loadtest-job.yaml"

    if [[ -n "${ECR_REGISTRY:-}" ]]; then
        kubectl set image job/pgctl-loadtest \
            loadtest="$ECR_REGISTRY/$IMAGE_NAME:$IMAGE_TAG" \
            -n "$NAMESPACE"
    fi

    log "Job started. Tailing logs..."
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=pgctl-loadtest -n "$NAMESPACE" --timeout=120s || true
    kubectl logs -f job/pgctl-loadtest -c loadtest -n "$NAMESPACE" || true
}

check_progress() {
    log "Fetching checkpoint..."
    POD=$(kubectl get pods -l app.kubernetes.io/name=pgctl-loadtest -n "$NAMESPACE" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [[ -z "$POD" ]]; then
        log "No load test pod found"
        return 1
    fi

    # Copy checkpoint file
    CHECKPOINT_FILE="/tmp/pgctl-checkpoint-$(date +%Y%m%d-%H%M%S).json"
    if kubectl cp "$NAMESPACE/$POD:/results/checkpoint.json" "$CHECKPOINT_FILE" -c loadtest 2>/dev/null; then
        log "Checkpoint saved to $CHECKPOINT_FILE"
        echo
        python3 -m json.tool "$CHECKPOINT_FILE" 2>/dev/null || cat "$CHECKPOINT_FILE"
    else
        log "No checkpoint file yet — test may still be starting"
    fi

    echo
    log "Recent logs:"
    kubectl logs "pod/$POD" -c loadtest -n "$NAMESPACE" --tail=20
}

show_status() {
    log "Job status:"
    kubectl get job pgctl-loadtest -n "$NAMESPACE" -o wide 2>/dev/null || echo "No job found"
    echo
    log "Pods:"
    kubectl get pods -l app.kubernetes.io/name=pgctl-loadtest -n "$NAMESPACE" -o wide 2>/dev/null || echo "No pods found"
    echo
    log "CloudWatch dashboard:"
    cd "$SCRIPT_DIR/terraform"
    terraform output -raw cloudwatch_dashboard_url 2>/dev/null || echo "Run 'terraform apply' first"
    echo
    cd "$SCRIPT_DIR"
}

fetch_results() {
    log "Fetching results..."
    POD=$(kubectl get pods -l app.kubernetes.io/name=pgctl-loadtest -n "$NAMESPACE" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [[ -n "$POD" ]]; then
        kubectl cp "$NAMESPACE/$POD:/results/report.json" "$SCRIPT_DIR/results-$(date +%Y%m%d-%H%M%S).json" -c loadtest 2>/dev/null || \
            log "No results file found (Job may still be running — try 'check' instead)"
        kubectl logs "pod/$POD" -c loadtest -n "$NAMESPACE" --tail=50
    else
        log "No load test pod found"
    fi
}

teardown() {
    log "Tearing down..."
    kubectl delete namespace "$NAMESPACE" --ignore-not-found
    cd "$SCRIPT_DIR/terraform"
    terraform destroy -auto-approve
    cd "$SCRIPT_DIR"
    log "Teardown complete"
}

COMMAND="${2:-all}"

case "$COMMAND" in
    setup)
        terraform_apply
        configure_kubectl
        apply_k8s_manifests
        show_status
        ;;
    run)
        configure_kubectl
        apply_k8s_manifests
        build_and_push
        run_job
        ;;
    check)
        check_progress
        ;;
    status)
        show_status
        ;;
    results)
        fetch_results
        ;;
    teardown)
        configure_kubectl
        teardown
        ;;
    all)
        terraform_apply
        configure_kubectl
        apply_k8s_manifests
        build_and_push
        run_job
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        echo "Unknown command: $COMMAND"
        usage
        exit 1
        ;;
esac
