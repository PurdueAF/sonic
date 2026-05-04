#!/usr/bin/env bash
# Runs inside the driver Job (no custom image). Needs outbound HTTPS to dl.k8s.io and PyPI.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

KUBECTL_VERSION="${KUBECTL_VERSION:-v1.29.13}"
case "$(uname -m)" in
  x86_64) KARCH=amd64 ;;
  aarch64) KARCH=arm64 ;;
  *)
    echo "unsupported architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

apt-get update -qq
apt-get install -y --no-install-recommends curl ca-certificates
curl -fsSL -o /usr/local/bin/kubectl \
  "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${KARCH}/kubectl"
chmod +x /usr/local/bin/kubectl
apt-get purge -y curl
apt-get autoremove -y
rm -rf /var/lib/apt/lists/*

pip install --no-cache-dir pandas pyarrow pyyaml

exec python /chep/run_experiment.py --config "${CHEP2026_CONFIG:-/config/config.yaml}"
