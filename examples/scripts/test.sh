#!/usr/bin/env bash
set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "${GREEN}PASS${NC}: $1"; }
fail() { echo -e "${RED}FAIL${NC}: $1"; exit 1; }

# ============================================================
# Test etcd controller
# ============================================================
echo ""
echo "========================================="
echo "  Testing etcd controller"
echo "========================================="

# Clean up any previous test resources.
kubectl delete greeting --all --ignore-not-found 2>/dev/null || true
kubectl delete greetingcard --all --ignore-not-found 2>/dev/null || true
kubectl delete greetingpolicy --all --ignore-not-found 2>/dev/null || true
sleep 2

# 1. Create GreetingPolicy.
echo "Creating GreetingPolicy 'spanish' with prefix 'Hola'..."
kubectl apply -f - <<'EOF'
apiVersion: greeting.example.com/v1alpha1
kind: GreetingPolicy
metadata:
  name: spanish
  namespace: default
spec:
  prefix: "Hola"
EOF
sleep 1

# 2. Create Greeting.
echo "Creating Greeting 'world' with name 'World'..."
kubectl apply -f - <<'EOF'
apiVersion: greeting.example.com/v1alpha1
kind: Greeting
metadata:
  name: world
  namespace: default
spec:
  name: "World"
EOF

# 3. Wait for status.
echo "Waiting for Greeting to become Ready..."
if ! kubectl wait greeting/world --for=jsonpath='{.status.phase}'=Ready --timeout=30s 2>/dev/null; then
    kubectl get greeting world -o yaml
    fail "Greeting did not reach Ready phase"
fi
pass "Greeting reached Ready phase"

# 4. Verify message.
MSG=$(kubectl get greeting world -o jsonpath='{.status.message}')
if [ "$MSG" = "Hola, World!" ]; then
    pass "Message is '$MSG'"
else
    fail "Expected 'Hola, World!' but got '$MSG'"
fi

# 5. Verify GreetingCard exists.
CARD_MSG=$(kubectl get greetingcard world-card -o jsonpath='{.spec.message}' 2>/dev/null || echo "NOT_FOUND")
if [ "$CARD_MSG" = "Hola, World!" ]; then
    pass "GreetingCard has correct message"
else
    fail "GreetingCard message: expected 'Hola, World!' got '$CARD_MSG'"
fi

# 6. Update policy prefix.
echo "Updating GreetingPolicy prefix to 'Bonjour'..."
kubectl patch greetingpolicy spanish --type=merge -p '{"spec":{"prefix":"Bonjour"}}'
echo "Waiting for re-reconciliation..."
for i in $(seq 1 20); do
    MSG=$(kubectl get greeting world -o jsonpath='{.status.message}' 2>/dev/null || echo "")
    if [ "$MSG" = "Bonjour, World!" ]; then
        break
    fi
    sleep 1
done
if [ "$MSG" = "Bonjour, World!" ]; then
    pass "Message updated to '$MSG' after policy change"
else
    fail "Expected 'Bonjour, World!' after policy change, got '$MSG'"
fi

echo ""
echo "========================================="
echo "  etcd controller: ALL TESTS PASSED"
echo "========================================="

# ============================================================
# Test postgres controller
# ============================================================
echo ""
echo "========================================="
echo "  Testing postgres controller"
echo "========================================="

# Port-forward the HTTP API.
kubectl port-forward svc/greeting-postgres-controller 8080:8080 &
PF_PID=$!
trap "kill $PF_PID 2>/dev/null || true" EXIT
sleep 3

API="http://localhost:8080/namespaces/default"

# 1. Create GreetingPolicy.
echo "Creating GreetingPolicy 'spanish' with prefix 'Hola'..."
STATUS=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/greetingpolicies" \
    -H 'Content-Type: application/json' \
    -d '{"metadata":{"name":"spanish"},"spec":{"prefix":"Hola"}}')
if [ "$STATUS" = "201" ]; then
    pass "GreetingPolicy created (HTTP $STATUS)"
else
    fail "GreetingPolicy creation failed (HTTP $STATUS)"
fi

sleep 1

# 2. Create Greeting.
echo "Creating Greeting 'world' with name 'World'..."
STATUS=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/greetings" \
    -H 'Content-Type: application/json' \
    -d '{"metadata":{"name":"world"},"spec":{"name":"World"}}')
if [ "$STATUS" = "201" ]; then
    pass "Greeting created (HTTP $STATUS)"
else
    fail "Greeting creation failed (HTTP $STATUS)"
fi

# 3. Wait for reconciliation.
echo "Waiting for Greeting to become Ready..."
for i in $(seq 1 30); do
    PHASE=$(curl -s "$API/greetings/world" | jq -r '.status.phase // empty' 2>/dev/null || echo "")
    if [ "$PHASE" = "Ready" ]; then
        break
    fi
    sleep 1
done
if [ "$PHASE" = "Ready" ]; then
    pass "Greeting reached Ready phase"
else
    fail "Greeting did not reach Ready phase after 30s"
fi

# 4. Verify message.
MSG=$(curl -s "$API/greetings/world" | jq -r '.status.message')
if [ "$MSG" = "Hola, World!" ]; then
    pass "Message is '$MSG'"
else
    fail "Expected 'Hola, World!' but got '$MSG'"
fi

# 5. Verify GreetingCard.
CARD_MSG=$(curl -s "$API/greetingcards/world-card" | jq -r '.spec.message // "NOT_FOUND"')
if [ "$CARD_MSG" = "Hola, World!" ]; then
    pass "GreetingCard has correct message"
else
    fail "GreetingCard message: expected 'Hola, World!' got '$CARD_MSG'"
fi

# 6. Update policy.
echo "Updating GreetingPolicy prefix to 'Bonjour'..."
curl -s -X PUT "$API/greetingpolicies/spanish" \
    -H 'Content-Type: application/json' \
    -d '{"metadata":{"name":"spanish"},"spec":{"prefix":"Bonjour"}}' > /dev/null

echo "Waiting for re-reconciliation..."
for i in $(seq 1 20); do
    MSG=$(curl -s "$API/greetings/world" | jq -r '.status.message // empty' 2>/dev/null || echo "")
    if [ "$MSG" = "Bonjour, World!" ]; then
        break
    fi
    sleep 1
done
if [ "$MSG" = "Bonjour, World!" ]; then
    pass "Message updated to '$MSG' after policy change"
else
    fail "Expected 'Bonjour, World!' after policy change, got '$MSG'"
fi

# 7. Test validation — should reject invalid spec.
echo "Testing CRD validation (empty name should fail)..."
STATUS=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/greetings" \
    -H 'Content-Type: application/json' \
    -d '{"metadata":{"name":"bad"},"spec":{"name":""}}')
if [ "$STATUS" = "422" ]; then
    pass "Validation rejected empty name (HTTP $STATUS)"
else
    fail "Expected HTTP 422 for empty name, got $STATUS"
fi

echo ""
echo "========================================="
echo "  postgres controller: ALL TESTS PASSED"
echo "========================================="

echo ""
echo "========================================="
echo "  ALL TESTS PASSED"
echo "========================================="
