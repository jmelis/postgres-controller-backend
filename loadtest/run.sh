#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPEC_FILE="${1:-specs/5k-baseline.yaml}"
TF_DIR="$SCRIPT_DIR/terraform"

usage() {
    cat <<EOF
Usage: $0 [SPEC_FILE] [COMMAND]

Commands:
  setup       Provision infrastructure (Terraform) and deploy harness
  run         Build, upload, and start the load test on EC2
  check       Fetch current checkpoint (mid-run progress)
  status      Show instance status and CloudWatch dashboard URL
  results     Fetch final results from the EC2 instance
  ssh         Open an SSH session to the harness instance
  teardown    Destroy everything (Terraform)
  all         Run setup + run (default if no command given)

Arguments:
  SPEC_FILE   Path to the YAML test spec (default: specs/5k-baseline.yaml)

Examples:
  $0 specs/5k-baseline.yaml all
  $0 specs/ceiling-hunt.yaml run
  $0 check
  $0 teardown
EOF
}

log() { echo "==> $*"; }

get_instance_ip() {
    cd "$TF_DIR"
    terraform output -raw ec2_instance_ip 2>/dev/null
}

get_ssh_key() {
    cd "$TF_DIR"
    local key_name
    key_name=$(terraform output -raw ssh_command 2>/dev/null | grep -oP '(?<=-i ~/\.ssh/)[^ ]+(?=\.pem)' || true)
    if [[ -z "$key_name" ]]; then
        key_name=$(grep 'ec2_key_name' "$TF_DIR/terraform.tfvars" 2>/dev/null | sed 's/.*= *"\(.*\)"/\1/' || true)
    fi
    echo "$HOME/.ssh/${key_name}.pem"
}

remote() {
    local ip key
    ip=$(get_instance_ip)
    key=$(get_ssh_key)
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -i "$key" "ec2-user@$ip" "$@"
}

remote_copy() {
    local ip key
    ip=$(get_instance_ip)
    key=$(get_ssh_key)
    scp -o StrictHostKeyChecking=no -i "$key" "$@"
}

terraform_apply() {
    log "Provisioning infrastructure with Terraform..."
    cd "$TF_DIR"
    terraform init -upgrade
    terraform apply -auto-approve
    cd "$SCRIPT_DIR"

    log "Waiting for instance to be reachable..."
    local ip
    ip=$(get_instance_ip)
    for i in $(seq 1 30); do
        if remote true 2>/dev/null; then
            log "Instance is reachable"
            return
        fi
        echo "  waiting... ($i/30)"
        sleep 10
    done
    log "WARNING: instance may not be reachable yet"
}

deploy() {
    log "Cross-compiling harness binary..."
    cd "$SCRIPT_DIR/.."
    GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$SCRIPT_DIR/loadtest-bin" ./loadtest/cmd/loadtest/
    cd "$SCRIPT_DIR"

    local ip
    ip=$(get_instance_ip)

    log "Uploading harness binary, spec, and configs..."
    remote_copy \
        "$SCRIPT_DIR/loadtest-bin" \
        "$SCRIPT_DIR/$SPEC_FILE" \
        "$TF_DIR/cloudwatch-agent-config.json" \
        "$TF_DIR/prometheus.yaml" \
        "ec2-user@$ip:/tmp/"

    remote <<'SETUP'
        sudo mv /tmp/loadtest-bin /opt/loadtest/loadtest
        sudo chmod +x /opt/loadtest/loadtest
        sudo mv /tmp/spec.yaml /opt/loadtest/spec.yaml 2>/dev/null || sudo mv /tmp/*.yaml /opt/loadtest/spec.yaml

        # Configure and start CloudWatch Agent.
        sudo mkdir -p /opt/aws/amazon-cloudwatch-agent/etc
        sudo mv /tmp/cloudwatch-agent-config.json /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
        sudo mv /tmp/prometheus.yaml /opt/loadtest/prometheus.yaml
        sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
            -a fetch-config \
            -m ec2 \
            -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json \
            -s || true
SETUP

    rm -f "$SCRIPT_DIR/loadtest-bin"
    log "Deploy complete"
}

start_harness() {
    log "Starting harness on EC2..."
    cd "$TF_DIR"
    local dsn
    dsn=$(terraform output -raw rds_connection_string)
    cd "$SCRIPT_DIR"

    remote "sudo bash -c 'cat > /opt/loadtest/run.env << ENVEOF
PGCTL_DSN=$dsn
ENVEOF'"

    remote <<'RUN'
        # Stop any existing run.
        sudo pkill -f '/opt/loadtest/loadtest' 2>/dev/null || true
        sleep 1

        # Start the harness in the background.
        sudo bash -c '
            source /opt/loadtest/run.env
            export PGCTL_DSN
            nohup /opt/loadtest/loadtest \
                --spec=/opt/loadtest/spec.yaml \
                --metrics-addr=:9090 \
                --report=/opt/loadtest/results/report.json \
                > /opt/loadtest/results/harness.log 2>&1 &
            echo $! > /opt/loadtest/harness.pid
        '
RUN

    log "Harness started. Tailing logs (Ctrl+C to detach)..."
    remote "sudo tail -f /opt/loadtest/results/harness.log" || true
}

check_progress() {
    log "Fetching checkpoint..."
    local ip
    ip=$(get_instance_ip)
    local checkpoint="/tmp/pgctl-checkpoint-$(date +%Y%m%d-%H%M%S).json"

    if remote_copy "ec2-user@$ip:/opt/loadtest/results/checkpoint.json" "$checkpoint" 2>/dev/null; then
        log "Checkpoint saved to $checkpoint"
        echo
        python3 -m json.tool "$checkpoint" 2>/dev/null || cat "$checkpoint"
    else
        log "No checkpoint file yet — test may still be starting"
    fi

    echo
    log "Recent logs:"
    remote "sudo tail -20 /opt/loadtest/results/harness.log" 2>/dev/null || log "No logs yet"
}

show_status() {
    log "Harness process:"
    remote "ps aux | grep '/opt/loadtest/loadtest' | grep -v grep" 2>/dev/null || echo "  Not running"

    echo
    log "CloudWatch dashboard:"
    cd "$TF_DIR"
    terraform output -raw cloudwatch_dashboard_url 2>/dev/null || echo "Run setup first"
    echo
    cd "$SCRIPT_DIR"
}

fetch_results() {
    log "Fetching results..."
    local ip
    ip=$(get_instance_ip)
    local results="$SCRIPT_DIR/results-$(date +%Y%m%d-%H%M%S).json"

    if remote_copy "ec2-user@$ip:/opt/loadtest/results/report.json" "$results" 2>/dev/null; then
        log "Results saved to $results"
        python3 -m json.tool "$results" 2>/dev/null || cat "$results"
    else
        log "No results file — test may still be running (try 'check')"
    fi

    echo
    log "Final logs:"
    remote "sudo tail -50 /opt/loadtest/results/harness.log" 2>/dev/null || true
}

open_ssh() {
    local ip key
    ip=$(get_instance_ip)
    key=$(get_ssh_key)
    log "Connecting to $ip..."
    exec ssh -o StrictHostKeyChecking=no -i "$key" "ec2-user@$ip"
}

teardown() {
    log "Tearing down..."
    cd "$TF_DIR"
    terraform destroy -auto-approve
    cd "$SCRIPT_DIR"
    log "Teardown complete"
}

COMMAND="${2:-all}"

case "$COMMAND" in
    setup)
        terraform_apply
        deploy
        show_status
        ;;
    run)
        deploy
        start_harness
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
    ssh)
        open_ssh
        ;;
    teardown)
        teardown
        ;;
    all)
        terraform_apply
        deploy
        start_harness
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
