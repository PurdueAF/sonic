#!/bin/bash
# Triton 24.11 + RecoBTag-only — Slurm resources aligned with SONIC Triton pods:
#   resources/cms-run3-miniaod/values.yaml  → 1 GPU, 2 CPU, 8G
#   resources/supersonic-interlink/values.yaml → same; InterLink vk flags → cms, hammer-nodes, 1 GPU, 1 day walltime
#
# Runs Triton inside Apptainer/Singularity from a local NGC-equivalent .sif (host EL9 glibc is too old
# to run /opt/tritonserver/bin/tritonserver directly).
#
# After start: triton_host.txt is written under SLURM_SUBMIT_DIR (see below).
#
# GPU: with #SBATCH --gres=gpu:1, Slurm / the prolog typically sets CUDA_VISIBLE_DEVICES to the
# allocated device(s). Do not force =0 here—that can mask the real device id or break MIG setups.
#
# If apptainer is not in default PATH, either: module load apptainer (uncomment below) or
#   export APPTAINER_BIN=/path/to/apptainer

#SBATCH --job-name=triton
#SBATCH --account=cms
#SBATCH --partition=hammer-nodes
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --gres=gpu:1
#SBATCH --time=1-00:00:00

set -euo pipefail

export SLURM_SUBMIT_DIR=/home/dkondra

# module load apptainer 2>/dev/null || true

REPO=/cvmfs/cms.cern.ch/el9_amd64_gcc13/cms/cmssw/CMSSW_16_0_0_pre4/external/el9_amd64_gcc13/data/RecoBTag/Combined/data/models/
# Override on sbatch line:  --export=ALL,TRITON_SIF=/other/path.sif
TRITON_SIF="${TRITON_SIF:-/depot/cms/users/yao317/Singularities/tritonserver_24.11-py3.sif}"

if [[ ! -f "$TRITON_SIF" ]]; then
  echo "ERROR: Triton SIF not found: $TRITON_SIF" >&2
  exit 1
fi

CTR="${APPTAINER_BIN:-}"
if [[ -z "$CTR" ]]; then
  if command -v apptainer >/dev/null 2>&1; then
    CTR=apptainer
  elif command -v singularity >/dev/null 2>&1; then
    CTR=singularity
  fi
fi
if [[ -z "$CTR" ]]; then
  echo "ERROR: apptainer or singularity required on PATH (host glibc cannot run this tritonserver binary)." >&2
  echo "Try: module load apptainer" >&2
  exit 1
fi

hostname -f >"${SLURM_SUBMIT_DIR}/triton_host.txt"
echo "Wrote ${SLURM_SUBMIT_DIR}/triton_host.txt"
cat "${SLURM_SUBMIT_DIR}/triton_host.txt"

# --nv: GPU + driver userspace inside container; bind /cvmfs for the model repository.
exec "$CTR" exec --nv \
  -B /cvmfs:/cvmfs \
  "$TRITON_SIF" \
  /opt/tritonserver/bin/tritonserver \
  --model-repository="${REPO}" \
  --allow-gpu-metrics=true \
  --log-verbose=0 \
  --strict-model-config=false \
  --exit-timeout-secs=60 \
  --grpc-port=8001 \
  --http-port=8000
