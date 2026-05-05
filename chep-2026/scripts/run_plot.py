#!/usr/bin/env python3
"""
Build CMS-style throughput vs latency plots from saved trial data (no Kubernetes).

Uses the newest ``results/chep2026_sonic_*/trials.csv`` under the CHEP tree,
or ``--trials path/to/trials.csv`` (e.g. ``live-plot-runs/chep2026_live_*/trials_for_plot.csv`` from ``watch_experiment_and_plot.sh``).

From the ``chep-2026`` directory after ``pixi install``::

    pixi run plot
    pixi run plot -- --trials ./downloaded/chep2026_sonic_*/trials.csv
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if (_SCRIPT_DIR.parent / "src" / "analysis.py").is_file():
    CHEP_ROOT = _SCRIPT_DIR.parent
    _SRC = CHEP_ROOT / "src"
else:
    CHEP_ROOT = _SCRIPT_DIR
    _SRC = CHEP_ROOT
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from analysis import aggregate_summary, load_trials, plot_throughput_vs_latency, variant_labels_from_trials

os.environ.setdefault("MPLCONFIGDIR", str(CHEP_ROOT / ".mplconfig"))
Path(os.environ["MPLCONFIGDIR"]).mkdir(parents=True, exist_ok=True)


def latest_trials_csv() -> Path:
    root = CHEP_ROOT / "results"
    if not root.is_dir():
        raise SystemExit(f"No results directory at {root}; run pixi run experiment first.")
    candidates = sorted(
        root.glob("chep2026_sonic_*/trials.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise SystemExit(
            f"No trials.csv under {root}/chep2026_sonic_*/; run pixi run experiment first."
        )
    return candidates[0]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--trials",
        type=Path,
        default=None,
        help="Path to trials.csv (default: newest under results/chep2026_sonic_*/)",
    )
    args = parser.parse_args()
    trials_path = args.trials.resolve() if args.trials else latest_trials_csv()
    if not trials_path.is_file():
        raise SystemExit(f"Not a file: {trials_path}")
    df = load_trials(trials_path)
    if df.empty:
        raise SystemExit("trials table is empty")

    summary = aggregate_summary(df)
    summary_out = trials_path.parent / "summary_by_batch.csv"
    summary_out.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_out, index=False)

    labels = variant_labels_from_trials(df)
    out = trials_path.parent / "throughput_vs_latency.png"
    plot_throughput_vs_latency(summary, labels, out, title_suffix="")
    print(f"Using {trials_path}")
    print(f"Wrote {out} (+ .pdf) and {summary_out}")


if __name__ == "__main__":
    main()
