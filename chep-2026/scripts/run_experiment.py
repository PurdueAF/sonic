#!/usr/bin/env python3
"""
CHEP 2026: throughput vs latency for ParticleNet (particlenet_AK4_PT) via perf_analyzer
gRPC :8001 to either SuperSONIC Envoy or bare Triton (per-variant ``grpc_via`` in config).

Plots: ``run_plot``. Slurm: ``slurm/triton_reco_btag.sbatch`` + CHEP2026_SLURM_TRITON_HOST.

From the ``chep-2026`` directory::

    pixi install
    pixi run experiment                  # run perf jobs from this machine (kubectl → cluster)
    pixi run experiment-k8s              # submit the driver Job only; experiment runs in-cluster

Or ``python scripts/run_experiment.py --where local|k8s`` (default: ``local``, or env
``CHEP2026_WHERE``). Inside the driver pod, run with ``--where local`` (default).

In-cluster driver pod: results go to the PVC via ``CHEP2026_OUTPUT_ROOT=/results``; see ``k8s/``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

_SCRIPT_DIR = Path(__file__).resolve().parent
# Repo checkout: scripts/run_experiment.py + src/*.py. In-cluster ConfigMap: flat /chep/*.py.
if (_SCRIPT_DIR.parent / "src" / "analysis.py").is_file():
    CHEP_ROOT = _SCRIPT_DIR.parent
    _SRC = CHEP_ROOT / "src"
else:
    CHEP_ROOT = _SCRIPT_DIR
    _SRC = CHEP_ROOT
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from analysis import build_trial_dataframe
from parse_perf import parse_perf_analyzer_log


def _resolve_under_root(path: Path | str, root: Path) -> Path:
    p = Path(path)
    return p if p.is_absolute() else (root / p)


def resolve_output_root(cfg: dict) -> Path:
    """Directory under which ``chep2026_sonic_<run_id>/`` is created. Env wins over YAML."""
    env = os.environ.get("CHEP2026_OUTPUT_ROOT", "").strip()
    if env:
        return Path(env)
    raw = cfg.get("output_root")
    if raw:
        return _resolve_under_root(raw, CHEP_ROOT)
    return CHEP_ROOT / "results"


def _kubectl(args: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["kubectl", *args],
        text=True,
        capture_output=True,
        check=False,
        **kwargs,
    )


def _kubectl_check() -> None:
    p = _kubectl(["version", "--client=true"])
    if p.returncode != 0:
        raise RuntimeError(
            "kubectl not found or not working; install kubectl or use --where local "
            "only from a machine with cluster access."
        )


def _load_k8s_documents(path: Path) -> list[dict]:
    docs: list[dict] = []
    for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
        if doc:
            docs.append(doc)
    return docs


def _dump_k8s_documents(docs: list[dict]) -> str:
    parts: list[str] = []
    for d in docs:
        blob = yaml.safe_dump(
            d, sort_keys=False, default_flow_style=False, allow_unicode=True
        ).rstrip()
        parts.append(blob)
    return "\n---\n".join(parts) + "\n"


def _set_namespace_on_docs(docs: list[dict], namespace: str) -> None:
    for d in docs:
        meta = d.get("metadata")
        if isinstance(meta, dict):
            meta["namespace"] = namespace


def _kubectl_apply_yaml(yaml_blob: str, *, dry_run_client: bool) -> None:
    cmd = ["kubectl", "apply"]
    if dry_run_client:
        cmd.append("--dry-run=client")
    cmd.extend(["-f", "-"])
    p = subprocess.run(cmd, input=yaml_blob, text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr or p.stdout or "kubectl apply failed")


def _kubectl_create_configmap_from_files(
    namespace: str,
    name: str,
    file_pairs: list[tuple[str, Path]],
    *,
    dry_run_client: bool,
) -> None:
    cmd = [
        "kubectl",
        "-n",
        namespace,
        "create",
        "configmap",
        name,
        "--dry-run=client",
        "-o",
        "yaml",
    ]
    for key, path in file_pairs:
        if not path.is_file():
            raise FileNotFoundError(path)
        cmd.extend(["--from-file", f"{key}={path}"])
    p1 = subprocess.run(cmd, capture_output=True, text=True)
    if p1.returncode != 0:
        raise RuntimeError(
            f"kubectl create configmap {name} (dry-run): {p1.stderr or p1.stdout}"
        )
    if dry_run_client:
        print(p1.stdout)
        return
    p2 = subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=p1.stdout,
        text=True,
        capture_output=True,
    )
    if p2.returncode != 0:
        raise RuntimeError(p2.stderr or p2.stdout)


def _inject_slurm_triton_host_env(job_doc: dict) -> None:
    host = os.environ.get("CHEP2026_SLURM_TRITON_HOST", "").strip()
    if not host:
        return
    c0 = job_doc["spec"]["template"]["spec"]["containers"][0]
    env_list = c0.setdefault("env", [])
    env_list = [e for e in env_list if e.get("name") != "CHEP2026_SLURM_TRITON_HOST"]
    env_list.append({"name": "CHEP2026_SLURM_TRITON_HOST", "value": host})
    c0["env"] = env_list


def submit_k8s_driver(
    cfg: dict,
    config_path: Path,
    *,
    dry_run_client: bool,
    skip_prereq_manifests: bool,
) -> None:
    """
    From your laptop: apply RBAC/PVC, refresh ConfigMaps, submit chep2026-experiment-driver.
    Namespace comes from cfg['namespace'] and overrides metadata in k8s/*.yaml on apply.
    """
    ns = str(cfg.get("namespace", "")).strip()
    if not ns:
        raise ValueError("config: namespace is required for --where k8s")

    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        raise RuntimeError(
            "Refusing --where k8s from inside a Kubernetes pod (would nest driver Jobs). "
            "Use --where local here."
        )

    k8s_dir = CHEP_ROOT / "k8s"
    if not k8s_dir.is_dir():
        raise FileNotFoundError(f"missing {k8s_dir} (k8s submit only works from a repo checkout)")

    if not dry_run_client:
        _kubectl_check()

    if not skip_prereq_manifests:
        for fname in ("driver-rbac.yaml", "pvc.yaml"):
            path = k8s_dir / fname
            docs = _load_k8s_documents(path)
            _set_namespace_on_docs(docs, ns)
            blob = _dump_k8s_documents(docs)
            if dry_run_client:
                print(f"# --- kubectl apply -f -  (from {path.name})\n{blob}")
            else:
                _kubectl_apply_yaml(blob, dry_run_client=False)

    driver_files: list[tuple[str, Path]] = [
        ("run_experiment.py", CHEP_ROOT / "scripts" / "run_experiment.py"),
        ("analysis.py", CHEP_ROOT / "src" / "analysis.py"),
        ("parse_perf.py", CHEP_ROOT / "src" / "parse_perf.py"),
        ("perf_job.yaml.template", CHEP_ROOT / "k8s" / "perf_job.yaml.template"),
        ("driver-entrypoint.sh", k8s_dir / "driver-entrypoint.sh"),
    ]
    _kubectl_create_configmap_from_files(
        ns,
        "chep2026-driver-code",
        driver_files,
        dry_run_client=dry_run_client,
    )

    exp_pairs = [("config.yaml", config_path.resolve())]
    _kubectl_create_configmap_from_files(
        ns,
        "chep2026-experiment-config",
        exp_pairs,
        dry_run_client=dry_run_client,
    )

    job_path = k8s_dir / "driver-job.yaml"
    job_docs = _load_k8s_documents(job_path)
    _set_namespace_on_docs(job_docs, ns)
    for d in job_docs:
        if d.get("kind") == "Job":
            _inject_slurm_triton_host_env(d)
    job_blob = _dump_k8s_documents(job_docs)

    if dry_run_client:
        print(f"# --- kubectl apply -f -  (from {job_path.name})\n{job_blob}")
        print(
            "\n(dry-run: no changes applied; drop --dry-run to submit the driver Job.)",
            file=sys.stderr,
        )
        return

    _kubectl(
        [
            "delete",
            "job",
            "chep2026-experiment-driver",
            "-n",
            ns,
            "--ignore-not-found",
        ]
    )
    _kubectl_apply_yaml(job_blob, dry_run_client=False)
    print(
        f"Submitted job/chep2026-experiment-driver in namespace {ns!r}. "
        f"Logs: kubectl logs -n {ns} -f job/chep2026-experiment-driver",
        file=sys.stderr,
    )


def keda_preflight(namespace: str) -> None:
    """Warn if KEDA may scale Triton beyond a single replica during the sweep."""
    r = _kubectl(
        [
            "get",
            "scaledobject",
            "-n",
            namespace,
            "-o",
            "json",
        ]
    )
    if r.returncode != 0 or not r.stdout.strip():
        print(
            "[preflight] no ScaledObjects listed (ok if KEDA disabled).",
            file=sys.stderr,
        )
        return
    try:
        data = json.loads(r.stdout)
    except json.JSONDecodeError:
        return
    bad: list[str] = []
    for item in data.get("items", []):
        name = item["metadata"]["name"]
        spec = item.get("spec", {})
        min_r = spec.get("minReplicaCount")
        max_r = spec.get("maxReplicaCount")
        if min_r is None or max_r is None:
            continue
        if int(min_r) != int(max_r):
            bad.append(f"{name} (min={min_r} max={max_r})")
    if bad:
        print(
            "[preflight] WARNING: fix autoscaling to a single Triton replica for this study, "
            "e.g. set minReplicaCount == maxReplicaCount == 1 on:\n  "
            + "\n  ".join(bad),
            file=sys.stderr,
        )


def resolve_triton_endpoint_host(variant_key: str, vcfg: dict) -> str:
    """Bare Triton gRPC host. Slurm uses triton_grpc_host_env."""
    if vcfg.get("triton_grpc_host"):
        return str(vcfg["triton_grpc_host"]).strip()
    env_name = vcfg.get("triton_grpc_host_env")
    if not env_name:
        raise ValueError(
            f"variant {variant_key!r}: set triton_grpc_host or triton_grpc_host_env in config"
        )
    host = os.environ.get(str(env_name), "").strip()
    if not host:
        raise ValueError(
            f"variant {variant_key!r}: export {env_name}=<slurm-node-fqdn-or-ip> "
            "(reachable from perf_analyzer pods)"
        )
    return host


def resolve_envoy_endpoint_host(variant_key: str, vcfg: dict) -> str:
    host = str(vcfg.get("envoy_grpc_host", "")).strip()
    if not host:
        raise ValueError(
            f"variant {variant_key!r}: grpc_via is envoy but envoy_grpc_host is missing"
        )
    return host


def resolve_grpc_target(
    variant_key: str, vcfg: dict, default_via: str
) -> tuple[str, str, str]:
    """
    Returns (host, via_token, via_desc) where via_token is 'triton'|'envoy'
    and via_desc is a short label for job logs.
    """
    via = str(vcfg.get("grpc_via", default_via)).lower().strip()
    if via == "envoy":
        return (
            resolve_envoy_endpoint_host(variant_key, vcfg),
            "envoy",
            "(Envoy)",
        )
    if via == "triton":
        return (
            resolve_triton_endpoint_host(variant_key, vcfg),
            "triton",
            "(Triton direct)",
        )
    raise ValueError(
        f"variant {variant_key!r}: grpc_via must be 'triton' or 'envoy', not {via!r}"
    )


def sanitize_job_name(variant_key: str, batch: int, trial: int) -> str:
    """RFC 1123 subdomain (<=63 chars)."""
    vk = re.sub(r"[^a-z0-9-]+", "-", variant_key.lower()).strip("-")[:20]
    name = f"chep-{vk}-b{batch}-r{trial}"
    if len(name) > 63:
        name = name[:63].rstrip("-")
    return name


def render_job(
    template: str,
    *,
    job_name: str,
    namespace: str,
    variant_key: str,
    batch: int,
    trial: int,
    grpc_target_host: str,
    grpc_via_desc: str,
    grpc_port: int,
    model_name: str,
    image: str,
    request_count: int,
    concurrency: int,
    measurement_ms: int,
    stability_pct: int,
    max_trials: int,
    cpu: str,
    memory: str,
) -> str:
    return (
        template.replace("JOB_NAME_PLACEHOLDER", job_name)
        .replace("NAMESPACE_PLACEHOLDER", namespace)
        .replace("VARIANT_KEY_PLACEHOLDER", variant_key)
        .replace("BATCH_PLACEHOLDER", str(batch))
        .replace("TRIAL_PLACEHOLDER", str(trial))
        .replace("GRPC_TARGET_HOST_PLACEHOLDER", grpc_target_host)
        .replace("GRPC_VIA_DESC_PLACEHOLDER", grpc_via_desc)
        .replace("GRPC_PORT_PLACEHOLDER", str(grpc_port))
        .replace("MODEL_NAME_PLACEHOLDER", model_name)
        .replace("IMAGE_PLACEHOLDER", image)
        .replace("REQUEST_COUNT_PLACEHOLDER", str(request_count))
        .replace("CONCURRENCY_PLACEHOLDER", str(concurrency))
        .replace("MEASUREMENT_MS_PLACEHOLDER", str(measurement_ms))
        .replace("STABILITY_PCT_PLACEHOLDER", str(stability_pct))
        .replace("MAX_TRIALS_PLACEHOLDER", str(max_trials))
        .replace("CPU_PLACEHOLDER", cpu)
        .replace("MEMORY_PLACEHOLDER", memory)
    )


def apply_job(manifest: str) -> None:
    p = subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=manifest,
        text=True,
        capture_output=True,
    )
    if p.returncode != 0:
        raise RuntimeError(f"kubectl apply failed: {p.stderr or p.stdout}")


def wait_job_terminal(namespace: str, job_name: str, timeout_s: int) -> str:
    """Block until Job succeeds, fails, or times out. Returns succeeded|failed|timeout."""
    deadline = time.monotonic() + timeout_s
    interval = 5
    while time.monotonic() < deadline:
        p = subprocess.run(
            [
                "kubectl",
                "get",
                "job",
                job_name,
                "-n",
                namespace,
                "-o",
                "jsonpath={.status.succeeded}:{.status.failed}",
            ],
            text=True,
            capture_output=True,
        )
        if p.returncode != 0:
            time.sleep(interval)
            continue
        raw = (p.stdout or "").strip()
        if not raw:
            time.sleep(interval)
            continue
        parts = raw.split(":")
        succ = int(parts[0] or 0)
        fail = int(parts[1] or 0) if len(parts) > 1 else 0
        if succ > 0:
            return "succeeded"
        if fail > 0:
            return "failed"
        time.sleep(interval)
    return "timeout"


def job_logs(namespace: str, job_name: str) -> str:
    p = subprocess.run(
        ["kubectl", "logs", f"job/{job_name}", "-n", namespace],
        text=True,
        capture_output=True,
    )
    if p.returncode != 0:
        return f"(kubectl logs failed: {p.stderr})\n"
    return p.stdout


def delete_job(namespace: str, job_name: str) -> None:
    subprocess.run(
        ["kubectl", "delete", "job", job_name, "-n", namespace, "--ignore-not-found"],
        text=True,
        capture_output=True,
    )


def _default_job_template_path() -> Path:
    for candidate in (
        CHEP_ROOT / "k8s" / "perf_job.yaml.template",
        CHEP_ROOT / "perf_job.yaml.template",
    ):
        if candidate.is_file():
            return candidate
    return CHEP_ROOT / "k8s" / "perf_job.yaml.template"


def run_suite(cfg: dict, args: argparse.Namespace) -> Path:
    namespace = cfg["namespace"]
    default_tpl = _default_job_template_path()
    raw_tpl = cfg.get("job_template")
    template_path = _resolve_under_root(raw_tpl, CHEP_ROOT) if raw_tpl else default_tpl
    template = template_path.read_text()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = resolve_output_root(cfg) / f"chep2026_sonic_{run_id}"
    log_dir = out_dir / "logs"
    if not args.dry_run:
        log_dir.mkdir(parents=True, exist_ok=True)

    if not args.skip_preflight:
        keda_preflight(namespace)

    if not args.dry_run:
        print(
            f"[chep2026] driver run_id={run_id} namespace={namespace} "
            f"out_dir={out_dir} model={cfg['model_name']!r}",
            flush=True,
        )

    variants = cfg["variants"]
    batch_sizes = [int(x) for x in cfg["batch_sizes"]]
    repeats = int(cfg["repeats"])
    timeout_s = int(cfg.get("kubectl_job_timeout_seconds", 7200))

    rows: list[dict] = []
    plot_labels = {k: v.get("plot_label", k) for k, v in variants.items()}
    default_via = str(cfg.get("default_grpc_via", "triton")).lower().strip()

    for variant_key, vcfg in variants.items():
        if vcfg.get("enabled") is False:
            continue
        grpc_target_host, grpc_via_token, grpc_via_desc = resolve_grpc_target(
            variant_key, vcfg, default_via
        )
        for batch in batch_sizes:
            for trial in range(repeats):
                job_name = sanitize_job_name(variant_key, batch, trial)
                manifest = render_job(
                    template,
                    job_name=job_name,
                    namespace=namespace,
                    variant_key=variant_key,
                    batch=batch,
                    trial=trial,
                    grpc_target_host=grpc_target_host,
                    grpc_via_desc=grpc_via_desc,
                    grpc_port=int(cfg["grpc_port"]),
                    model_name=cfg["model_name"],
                    image=cfg["image"],
                    request_count=int(cfg["request_count"]),
                    concurrency=int(cfg["concurrency"]),
                    measurement_ms=int(cfg["measurement_interval_ms"]),
                    stability_pct=int(cfg.get("stability_percentage", 10)),
                    max_trials=int(cfg.get("max_measurement_trials", 10)),
                    cpu=str(cfg["resources"]["cpu"]),
                    memory=str(cfg["resources"]["memory"]),
                )
                if args.dry_run:
                    print(
                        f"[chep2026] dry-run job/{job_name} variant={variant_key!r} "
                        f"batch={batch} trial={trial} grpc={grpc_target_host}:{cfg['grpc_port']} "
                        f"via={grpc_via_token}",
                        flush=True,
                    )
                    print(manifest)
                    continue

                print(
                    f"[chep2026] starting job/{job_name} variant={variant_key!r} "
                    f"batch={batch} trial={trial} grpc={grpc_target_host}:{cfg['grpc_port']} "
                    f"via={grpc_via_token} plot_label={plot_labels[variant_key]!r}",
                    flush=True,
                )
                delete_job(namespace, job_name)
                apply_job(manifest)
                started = time.time()
                status = wait_job_terminal(namespace, job_name, timeout_s)
                if status != "succeeded":
                    print(
                        f"[chep2026] job/{job_name} variant={variant_key!r} batch={batch} "
                        f"ended status={status!r}",
                        flush=True,
                    )
                    log_txt = job_logs(namespace, job_name)
                    (log_dir / f"FAILED_{job_name}.log").write_text(
                        log_txt, encoding="utf-8"
                    )
                    delete_job(namespace, job_name)
                    raise RuntimeError(
                        f"job/{job_name} ended with {status!r} (logs in {log_dir})"
                    )

                log_txt = job_logs(namespace, job_name)
                (log_dir / f"{job_name}.log").write_text(log_txt, encoding="utf-8")
                metrics = parse_perf_analyzer_log(log_txt)
                flat = metrics.as_flat_dict()
                row = {
                    "run_id": run_id,
                    "utc_finished": datetime.now(timezone.utc).isoformat(),
                    "variant_key": variant_key,
                    "plot_label": plot_labels[variant_key],
                    "batch_size": batch,
                    "trial_index": trial,
                    "job_name": job_name,
                    "grpc_via": grpc_via_token,
                    "grpc_target_host": grpc_target_host,
                    "grpc_port": int(cfg["grpc_port"]),
                    "duration_sec": round(time.time() - started, 3),
                    **{k: v for k, v in flat.items() if k != "raw_log_excerpt"},
                }
                rows.append(row)
                delete_job(namespace, job_name)
                print(
                    f"[chep2026] finished job/{job_name} variant={variant_key!r} "
                    f"batch={batch} status=succeeded duration_sec={row['duration_sec']}",
                    flush=True,
                )

                df_partial = build_trial_dataframe(rows)
                df_partial.to_csv(out_dir / "trials_partial.csv", index=False)

    if args.dry_run:
        return out_dir

    df = build_trial_dataframe(rows)
    df.to_parquet(out_dir / "trials.parquet")
    df.to_csv(out_dir / "trials.csv", index=False)

    meta = {
        "run_id": run_id,
        "config": cfg,
        "rows": len(df),
        "plot_note": "Newest run is picked up by: pixi run plot",
    }
    (out_dir / "run_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"Run artifacts written to {out_dir}")
    print("Plot: pixi run plot")
    return out_dir


def _default_config_arg() -> Path:
    env = os.environ.get("CHEP2026_CONFIG", "").strip()
    if env:
        return Path(env)
    default = CHEP_ROOT / "config.yaml"
    if default.is_file():
        return default
    return default


def main() -> None:
    _where_env = os.environ.get("CHEP2026_WHERE", "local").strip().lower()
    if _where_env not in ("local", "k8s"):
        _where_env = "local"

    parser = argparse.ArgumentParser(description=__doc__)
    _default_cfg = _default_config_arg()
    parser.add_argument(
        "--config",
        type=Path,
        default=_default_cfg,
        help="Experiment YAML (default: CHEP2026_CONFIG or ./config.yaml)",
    )
    parser.add_argument(
        "--where",
        choices=("local", "k8s"),
        default=_where_env,
        help="local: drive perf_analyzer Jobs from this host; k8s: submit the in-cluster driver Job "
        "(use from laptop). Default: CHEP2026_WHERE or local.",
    )
    parser.add_argument(
        "--skip-k8s-prereqs",
        action="store_true",
        help="With --where k8s: skip applying driver-rbac.yaml and pvc.yaml.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="local: print perf_analyzer Job YAML only. k8s: kubectl apply --dry-run=client.",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip KEDA min/max replica warning.",
    )
    args = parser.parse_args()

    cfg = yaml.safe_load(args.config.read_text())
    if args.where == "k8s":
        submit_k8s_driver(
            cfg,
            args.config.resolve(),
            dry_run_client=args.dry_run,
            skip_prereq_manifests=args.skip_k8s_prereqs,
        )
    else:
        run_suite(cfg, args)
        if args.dry_run:
            print("(dry-run: no result directory created)")


if __name__ == "__main__":
    main()
