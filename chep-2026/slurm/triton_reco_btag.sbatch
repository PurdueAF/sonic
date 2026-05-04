#!/bin/bash
# Triton 24.11 + RecoBTag-only — Slurm resources aligned with SONIC Triton pods:
#   resources/cms-run3-miniaod/values.yaml  → 1 GPU, 2 CPU, 8G
#   resources/supersonic-interlink/values.yaml → same; InterLink vk flags → cms, hammer-nodes, 1 GPU, 1 day walltime
#
# After start: triton_host.txt is written under SLURM_SUBMIT_DIR (see below).
#
# SLURM_SUBMIT_DIR: absolute path to the directory you ran sbatch from; set automatically by Slurm
# for every batch job (not something you define in the script).
#
# GPU: with #SBATCH --gres=gpu:1, Slurm / the prolog typically sets CUDA_VISIBLE_DEVICES to the
# allocated device(s). Do not force =0 here—that can mask the real device id or break MIG setups.

#SBATCH --job-name=chep-triton
#SBATCH --account=cms
#SBATCH --partition=hammer-nodes
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --gres=gpu:1
#SBATCH --time=1-00:00:00

set -euo pipefail

REPO=/cvmfs/cms.cern.ch/el9_amd64_gcc13/cms/cmssw/CMSSW_16_0_0_pre4/external/el9_amd64_gcc13/data/RecoBTag/Combined/data/models/
TRITON_ROOT=/cvmfs/unpacked.cern.ch/nvcr.io/nvidia/tritonserver:24.11-py3

hostname -f >"${SLURM_SUBMIT_DIR}/triton_host.txt"
echo "Wrote ${SLURM_SUBMIT_DIR}/triton_host.txt"
cat "${SLURM_SUBMIT_DIR}/triton_host.txt"

exec "${TRITON_ROOT}/bin/tritonserver" \
  --model-repository="${REPO}" \
  --allow-gpu-metrics=true \
  --log-verbose=0 \
  --strict-model-config=false \
  --exit-timeout-secs=60 \
  --grpc-port=8001 \
  --http-port=8000
