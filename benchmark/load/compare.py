"""Compare baseline vs patched runs from a JSONL report.

Reads /tmp/workload_full.jsonl (or REPORT env override). Groups by the
``label`` prefix (everything before the first '_'). Prints per-op median
of medians + the delta. Highlights wins/losses past 5% with ↑↓.
"""
from __future__ import annotations

import json
import os
import statistics
from collections import defaultdict

REPORT = os.environ.get("REPORT", "/tmp/workload_full.jsonl")


def load(path: str) -> list[dict]:
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def group_by_label(rows: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        # label is e.g. "baseline_iter1"; strip the trailing _iterN
        lab = r.get("label", "?")
        prefix = lab.split("_iter")[0] if "_iter" in lab else lab
        out[prefix].append(r)
    return out


def median_of(rows: list[dict], op: str, field: str) -> float | None:
    vals = [r["ops"][op][field] for r in rows if op in r.get("ops", {}) and field in r["ops"][op]]
    return statistics.median(vals) if vals else None


def fmt_delta(base: float | None, patched: float | None, *, lower_is_better: bool = True) -> str:
    if base is None or patched is None or base == 0:
        return "n/a"
    delta_pct = (patched - base) / base * 100
    sign = "↓" if (delta_pct < 0) == lower_is_better else "↑"
    if abs(delta_pct) < 5:
        sign = "≈"
    return f"{delta_pct:+6.1f}% {sign}"


def _pair(lab: str) -> tuple[str, str] | None:
    """Split ``baseline-daytona`` → (baseline, daytona). Returns None if
    the label doesn't match a baseline/patched pair convention."""
    for prefix in ("baseline", "patched"):
        if lab == prefix:
            return prefix, ""
        if lab.startswith(prefix + "-") or lab.startswith(prefix + "_"):
            return prefix, lab[len(prefix) + 1:]
    return None


def _emit_comparison(base_lab: str, base_rows: list[dict], pat_lab: str, pat_rows: list[dict]) -> None:
    print(f"\n=== {base_lab} vs {pat_lab} ===")
    print(f"{'op':>22} {'base p50':>10} {'pat p50':>10} {'p50 Δ':>14}    {'base p99':>10} {'pat p99':>10} {'p99 Δ':>14}")
    ops = set()
    for r in base_rows + pat_rows:
        ops.update(r.get("ops", {}).keys())
    for op in sorted(ops):
        b50 = median_of(base_rows, op, "p50_ms")
        p50 = median_of(pat_rows, op, "p50_ms")
        b99 = median_of(base_rows, op, "p99_ms")
        p99 = median_of(pat_rows, op, "p99_ms")
        b50s = f"{b50:.1f}" if b50 is not None else "  n/a"
        p50s = f"{p50:.1f}" if p50 is not None else "  n/a"
        b99s = f"{b99:.1f}" if b99 is not None else "  n/a"
        p99s = f"{p99:.1f}" if p99 is not None else "  n/a"
        d50 = fmt_delta(b50, p50)
        d99 = fmt_delta(b99, p99)
        print(f"{op:>22} {b50s:>10} {p50s:>10} {d50:>14}    {b99s:>10} {p99s:>10} {d99:>14}")
    bw = statistics.median(r["wall_s"] for r in base_rows)
    pw = statistics.median(r["wall_s"] for r in pat_rows)
    bt = statistics.median(r["throughput_sessions_per_s"] for r in base_rows)
    pt = statistics.median(r["throughput_sessions_per_s"] for r in pat_rows)
    print(f"\n{'wall_s':>22} {bw:>10.2f} {pw:>10.2f} {fmt_delta(bw, pw):>14}")
    print(f"{'throughput sess/s':>22} {bt:>10.3f} {pt:>10.3f} {fmt_delta(bt, pt, lower_is_better=False):>14}")


def main() -> None:
    rows = load(REPORT)
    if not rows:
        print(f"No data in {REPORT}"); return

    groups = group_by_label(rows)
    labels = sorted(groups.keys())
    if len(labels) < 2:
        print(f"Need at least 2 labels in {REPORT}; found: {labels}")
        return

    print(f"Source: {REPORT}")
    print(f"Labels (groups by '_iter' split): {labels}")
    for lab in labels:
        n = len(groups[lab])
        wall = statistics.median(r["wall_s"] for r in groups[lab])
        tput = statistics.median(r["throughput_sessions_per_s"] for r in groups[lab])
        errs = sum(r.get("errors", 0) for r in groups[lab])
        print(f"  {lab}: iters={n}  median_wall={wall:.2f}s  median_tput={tput:.3f} sess/s  total_errors={errs}")

    # Auto-pair baseline-X with patched-X. Falls back to first-two-labels
    # when no baseline/patched naming convention is found.
    pairs: list[tuple[str, str]] = []
    suffixes: dict[str, dict[str, str]] = {}
    for lab in labels:
        sp = _pair(lab)
        if sp is None:
            continue
        prefix, suffix = sp
        suffixes.setdefault(suffix, {})[prefix] = lab
    for suffix, by_prefix in sorted(suffixes.items()):
        if "baseline" in by_prefix and "patched" in by_prefix:
            pairs.append((by_prefix["baseline"], by_prefix["patched"]))

    if not pairs:
        pairs = [(labels[0], labels[1])]

    for base_lab, pat_lab in pairs:
        _emit_comparison(base_lab, groups[base_lab], pat_lab, groups[pat_lab])


if __name__ == "__main__":
    main()
