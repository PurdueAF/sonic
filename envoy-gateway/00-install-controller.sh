#!/usr/bin/env bash
set -euo pipefail

ENVOY_GATEWAY_VERSION="v1.7.1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

kubectl apply --server-side -f \
  "https://github.com/envoyproxy/gateway/releases/download/${ENVOY_GATEWAY_VERSION}/install.yaml"

kubectl apply -f "${SCRIPT_DIR}/envoy-gateway-config.yaml"
kubectl apply -f "${SCRIPT_DIR}/rbac-gateway-namespace.yaml"

kubectl rollout restart deployment/envoy-gateway -n envoy-gateway-system

kubectl wait --timeout=5m \
  -n envoy-gateway-system \
  deployment/envoy-gateway \
  --for=condition=Available
