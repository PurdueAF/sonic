"""
Parse NVIDIA perf_analyzer stdout for throughput, client latency, transport, and server breakdown.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PerfMetrics:
    raw_log_excerpt: str = ""
    throughput_infer_per_sec: float | None = None
    # Client-reported headline (concurrency summary line)
    concurrency_summary_latency_usec: float | None = None
    # Client
    latency_avg_usec: float | None = None
    # Transport (HTTP or gRPC wording varies by version)
    transport_label: str | None = None
    transport_total_usec: float | None = None
    transport_send_recv_usec: float | None = None
    transport_response_wait_usec: float | None = None
    # gRPC-specific naming in some builds
    grpc_marshal_request_usec: float | None = None
    grpc_unmarshal_response_usec: float | None = None
    # Server-side request latency breakdown
    server_request_total_usec: float | None = None
    server_overhead_usec: float | None = None
    server_queue_usec: float | None = None
    server_compute_input_usec: float | None = None
    server_compute_infer_usec: float | None = None
    server_compute_output_usec: float | None = None
    # Capture any other key=value usec pairs we find
    extra: dict[str, float] = field(default_factory=dict)

    def as_flat_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        for k, v in self.__dict__.items():
            if k == "extra":
                continue
            d[k] = v
        for k, v in self.extra.items():
            d[f"extra_{k}"] = v
        return d


_FLOAT = r"(\d+(?:\.\d+)?)"
_USEC_PAIR = re.compile(
    rf"(?P<name>[a-z][a-z0-9 /_+-]+?)\s+(?P<val>{_FLOAT})\s*usec",
    re.IGNORECASE,
)

# Concurrency: 1, throughput: 123.4 infer/sec, latency 5678 usec
_CONCURRENCY_LINE = re.compile(
    rf"Concurrency:\s*\d+\s*,\s*throughput:\s*(?P<tput>{_FLOAT})\s*infer/sec\s*,\s*latency\s*(?P<lat>{_FLOAT})\s*usec",
    re.IGNORECASE,
)

_AVG_LATENCY = re.compile(rf"Avg latency:\s*(?P<v>{_FLOAT})\s*usec", re.IGNORECASE)

# Avg HTTP time: 700 usec (send/recv 102 usec + response wait 598 usec)
# Avg gRPC time: ... (...)
_TRANSPORT = re.compile(
    rf"Avg\s+(?P<label>HTTP|gRPC)\s+time:\s*(?P<total>{_FLOAT})\s*usec\s*\((?P<rest>[^)]+)\)",
    re.IGNORECASE,
)

# Avg request latency: 382 usec (overhead 41 usec + queue 41 usec + ...)
_SERVER_REQ = re.compile(
    rf"Avg request latency:\s*(?P<total>{_FLOAT})\s*usec\s*\((?P<rest>[^)]+)\)",
    re.IGNORECASE,
)


def _parse_breakdown_segment(segment: str, target: dict[str, float]) -> None:
    """Parse 'name 12 usec + name2 3 usec' fragments."""
    segment = segment.replace("\n", " ")
    for m in _USEC_PAIR.finditer(segment):
        name = re.sub(r"\s+", " ", m.group("name").strip().lower())
        name = re.sub(r"[^a-z0-9]+", "_", name).strip("_")
        if not name:
            continue
        target[name] = float(m.group("val"))


def parse_perf_analyzer_log(text: str, excerpt_chars: int = 24000) -> PerfMetrics:
    """Best-effort parse; missing fields stay None (Triton / perf_analyzer version dependent)."""
    m = PerfMetrics()
    t = text.replace("\r\n", "\n")
    m.raw_log_excerpt = t[-excerpt_chars:] if len(t) > excerpt_chars else t

    # Prefer last concurrency line (stable measurement)
    for line in t.splitlines():
        cm = _CONCURRENCY_LINE.search(line)
        if cm:
            m.throughput_infer_per_sec = float(cm.group("tput"))
            m.concurrency_summary_latency_usec = float(cm.group("lat"))

    am = _AVG_LATENCY.search(t)
    if am:
        m.latency_avg_usec = float(am.group("v"))

    tm = _TRANSPORT.search(t)
    if tm:
        m.transport_label = tm.group("label").upper()
        m.transport_total_usec = float(tm.group("total"))
        rest = tm.group("rest")
        chunk: dict[str, float] = {}
        _parse_breakdown_segment(rest, chunk)
        m.transport_send_recv_usec = chunk.pop("send_recv", m.transport_send_recv_usec)
        m.transport_response_wait_usec = chunk.pop("response_wait", m.transport_response_wait_usec)
        m.grpc_marshal_request_usec = chunk.pop("marshal_request", m.grpc_marshal_request_usec)
        m.grpc_unmarshal_response_usec = chunk.pop(
            "unmarshal_response", m.grpc_unmarshal_response_usec
        )
        for k, v in chunk.items():
            m.extra[f"transport_{k}"] = v

    sm = _SERVER_REQ.search(t)
    if sm:
        m.server_request_total_usec = float(sm.group("total"))
        rest = sm.group("rest")
        chunk = {}
        _parse_breakdown_segment(rest, chunk)
        m.server_overhead_usec = chunk.pop("overhead", m.server_overhead_usec)
        m.server_queue_usec = chunk.pop("queue", m.server_queue_usec)
        m.server_compute_input_usec = chunk.pop("compute_input", m.server_compute_input_usec)
        m.server_compute_infer_usec = chunk.pop("compute_infer", m.server_compute_infer_usec)
        m.server_compute_output_usec = chunk.pop("compute_output", m.server_compute_output_usec)
        for k, v in chunk.items():
            m.extra[f"server_{k}"] = v

    if m.throughput_infer_per_sec is None:
        # Fallback: any "infer/sec" token
        for mm in re.finditer(rf"throughput:\s*(?P<t>{_FLOAT})\s*infer/sec", t, re.I):
            m.throughput_infer_per_sec = float(mm.group("t"))

    return m
