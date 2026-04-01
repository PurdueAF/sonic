#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

kubectl apply -f "${SCRIPT_DIR}/01-gatewayclass.yaml"
kubectl apply -f "${SCRIPT_DIR}/02-envoyproxy.yaml"
kubectl apply -f "${SCRIPT_DIR}/03-gateway.yaml"
kubectl apply -f "${SCRIPT_DIR}/04-grpcroute.yaml"
kubectl apply -f "${SCRIPT_DIR}/05-networkpolicy.yaml"
kubectl apply -f "${SCRIPT_DIR}/06-backend-traffic-policy.yaml"
kubectl apply -f "${SCRIPT_DIR}/07-podmonitor.yaml"
