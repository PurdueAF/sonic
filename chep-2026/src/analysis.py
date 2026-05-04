"""Aggregation and CMS-style plotting for CHEP 2026 perf_analyzer trial tables."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def latency_for_plot(row: pd.Series) -> float | None:
    if pd.notna(row.get("latency_avg_usec")):
        return float(row["latency_avg_usec"])
    if pd.notna(row.get("concurrency_summary_latency_usec")):
        return float(row["concurrency_summary_latency_usec"])
    return None


def build_trial_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["latency_plot_usec"] = df.apply(latency_for_plot, axis=1)
    return df


def aggregate_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    g = df.groupby(["variant_key", "batch_size"], as_index=False)
    out = g.agg(
        throughput_mean=("throughput_infer_per_sec", "mean"),
        throughput_std=("throughput_infer_per_sec", "std"),
        throughput_n=("throughput_infer_per_sec", "count"),
        latency_plot_mean=("latency_plot_usec", "mean"),
        latency_plot_std=("latency_plot_usec", "std"),
    )
    out["throughput_std"] = out["throughput_std"].fillna(0.0)
    out["latency_plot_std"] = out["latency_plot_std"].fillna(0.0)
    n = out["throughput_n"].clip(lower=1)
    out["throughput_sem"] = out["throughput_std"] / np.sqrt(n)
    out["latency_sem"] = out["latency_plot_std"] / np.sqrt(n)
    return out


def variant_labels_from_trials(df: pd.DataFrame) -> dict[str, str]:
    if "plot_label" in df.columns:
        return (
            df[["variant_key", "plot_label"]]
            .drop_duplicates()
            .set_index("variant_key")["plot_label"]
            .to_dict()
        )
    return {k: k for k in df["variant_key"].unique()}


def _series_legend_label(raw: str) -> str:
    """Remove legacy suffixes from stored plot_label (legend only)."""
    s = str(raw).strip()
    for fragment in (" (SuperSONIC Envoy)", "(SuperSONIC Envoy)"):
        if fragment in s:
            s = s.replace(fragment, "").strip()
    return s


def plot_throughput_vs_latency(
    summary: pd.DataFrame,
    variant_labels: dict[str, str],
    out_path: Path,
    *,
    title_suffix: str = "",
) -> None:
    import matplotlib.pyplot as plt
    import mplhep as hep

    hep.style.use("CMS")
    fig, ax = plt.subplots(figsize=(10.0, 8.0))
    colors = {
        "k8s": "#1f77b4",
        "interlink": "#d62728",
        "slurm": "#2ca02c",
    }
    offset_cycle = ((8, 8), (8, -14), (-10, 8), (-12, -10))
    for vi, (variant_key, sub) in enumerate(summary.groupby("variant_key")):
        label = _series_legend_label(variant_labels.get(variant_key, variant_key))
        c = colors.get(variant_key, "#333333")
        sub = sub.sort_values("batch_size")
        ax.plot(
            sub["throughput_mean"],
            sub["latency_plot_mean"],
            "-",
            color=c,
            alpha=0.55,
            linewidth=1.8,
            zorder=1,
        )
        ax.errorbar(
            sub["throughput_mean"],
            sub["latency_plot_mean"],
            xerr=sub["throughput_sem"],
            yerr=sub["latency_sem"],
            fmt="o",
            markersize=5,
            capsize=2,
            label=label,
            color=c,
            elinewidth=1.2,
            zorder=2,
        )
        ox, oy = offset_cycle[vi % len(offset_cycle)]
        for _, row in sub.iterrows():
            bs = row["batch_size"]
            bs_txt = str(int(bs)) if float(bs).is_integer() else str(bs)
            ax.annotate(
                f"b={bs_txt}",
                (row["throughput_mean"], row["latency_plot_mean"]),
                textcoords="offset points",
                xytext=(ox, oy),
                fontsize=11,
                color=c,
                alpha=0.95,
                zorder=3,
            )
    ax.set_xlabel("Throughput [infer / s]")
    ax.set_ylabel(r"Average batch latency [$\mu$s]")
    title = "SuperSONIC with and without interLink"
    if title_suffix:
        title = f"{title}\n{title_suffix}"
    ax.set_title(title, pad=22)
    hep.cms.label(data=False, text="Work in progress", loc=2, ax=ax)
    ax.legend(loc="best", frameon=True)
    ax.grid(True, which="major", linestyle=":", alpha=0.5)
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    fig.tight_layout(rect=(0.0, 0.0, 1.0, 0.94))
    out_path = Path(out_path)
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    fig.savefig(out_path.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)


def load_trials(path: Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    df["latency_plot_usec"] = df.apply(latency_for_plot, axis=1)
    return df
