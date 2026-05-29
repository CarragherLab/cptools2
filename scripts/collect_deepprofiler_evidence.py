"""Collect concise evidence from a DeepProfiler scalability run."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


ERROR_PATTERNS = (
    "Could not create cudnn handle",
    "CUDNN_STATUS_INTERNAL_ERROR",
    "Failed to get convolution algorithm",
    "bus error",
    "CUDA_VISIBLE_DEVICES=unset",
)

LOG_FILENAMES = {
    ".command.err": 0,
    ".command.log": 1,
    "launcher.log": 2,
}


def collect_trace_summary(trace_path: Path) -> dict[str, int]:
    summary = {
        "feature_total": 0,
        "feature_completed": 0,
        "feature_failed": 0,
        "feature_failed_attempts": 0,
        "cellpose_total": 0,
        "cellpose_completed": 0,
        "cellpose_failed": 0,
        "cellpose_failed_attempts": 0,
    }
    if not trace_path.exists():
        return summary

    final_statuses: dict[str, set[str]] = {}
    with trace_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            process = (row.get("process") or row.get("name") or "").strip()
            status = (row.get("status") or "").strip()
            if not process:
                continue

            if "FEATURE_EXTRACT" in process:
                final_statuses.setdefault(process, set()).add(status)
                if status == "FAILED":
                    summary["feature_failed_attempts"] += 1
            elif "CELLPOSE_SEGMENT" in process:
                final_statuses.setdefault(process, set()).add(status)
                if status == "FAILED":
                    summary["cellpose_failed_attempts"] += 1

    for process, statuses in final_statuses.items():
        completed = bool(statuses & {"COMPLETED", "CACHED"})
        failed = "FAILED" in statuses and not completed
        if "FEATURE_EXTRACT" in process:
            summary["feature_total"] += 1
            if completed:
                summary["feature_completed"] += 1
            elif failed:
                summary["feature_failed"] += 1
        elif "CELLPOSE_SEGMENT" in process:
            summary["cellpose_total"] += 1
            if completed:
                summary["cellpose_completed"] += 1
            elif failed:
                summary["cellpose_failed"] += 1

    return summary


def first_error_line(run_root: Path) -> str:
    candidates = []
    for path in run_root.rglob("*"):
        if path.is_file() and path.name in LOG_FILENAMES:
            candidates.append(path)

    for path in sorted(candidates, key=lambda item: (LOG_FILENAMES[item.name], item.as_posix())):
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line in lines:
            line_lower = line.lower()
            if any(pattern.lower() in line_lower for pattern in ERROR_PATTERNS):
                return line.strip()
    return ""


def output_counts(run_root: Path) -> dict[str, int]:
    published_npz = list(run_root.glob("outputs/*/features/**/*.npz"))
    npz_count = len(published_npz)
    if npz_count == 0:
        npz_count = len({path.name for path in run_root.rglob("*.npz") if path.is_file()})

    mask_names = set()
    no_cells_count = 0

    for path in run_root.rglob("*"):
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        name_lower = path.name.lower()
        if suffix in {".tif", ".tiff"} and "mask" in name_lower:
            mask_names.add(path.name)
        elif path.name == "no_cells.tsv":
            no_cells_count += 1

    return {"npz": npz_count, "masks": len(mask_names), "no_cells": no_cells_count}


def collect(run_root: Path) -> dict[str, object]:
    run_root = Path(run_root)
    trace_path = _find_trace_path(run_root)
    return {
        "run_root": str(run_root),
        "trace": collect_trace_summary(trace_path),
        "outputs": output_counts(run_root),
        "first_error": first_error_line(run_root),
    }


def _find_trace_path(run_root: Path) -> Path:
    direct = run_root / "trace.txt"
    if direct.exists():
        return direct
    candidates = sorted(run_root.rglob("trace*.txt"))
    return candidates[0] if candidates else direct


def render_markdown(summary: dict[str, object]) -> str:
    trace = summary["trace"]
    outputs = summary["outputs"]
    first_error = summary["first_error"] or "None detected"
    return "\n".join(
        [
            "# DeepProfiler Scalability Evidence",
            "",
            f"Run root: `{summary['run_root']}`",
            "",
            "## Trace",
            "",
            f"- FEATURE_EXTRACT: {trace['feature_completed']}/{trace['feature_total']} completed, {trace['feature_failed']} failed",
            f"- FEATURE_EXTRACT failed attempts: {trace['feature_failed_attempts']}",
            f"- CELLPOSE_SEGMENT: {trace['cellpose_completed']}/{trace['cellpose_total']} completed, {trace['cellpose_failed']} failed",
            f"- CELLPOSE_SEGMENT failed attempts: {trace['cellpose_failed_attempts']}",
            "",
            "## Outputs",
            "",
            f"- `.npz` files: {outputs['npz']}",
            f"- mask files: {outputs['masks']}",
            f"- `no_cells.tsv` files: {outputs['no_cells']}",
            "",
            "## First Error",
            "",
            first_error,
            "",
        ]
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect evidence from a DeepProfiler scalability run."
    )
    parser.add_argument("run_root", type=Path, help="root directory of the run")
    parser.add_argument("--json-out", type=Path, help="write JSON summary to this path")
    parser.add_argument("--md-out", type=Path, help="write Markdown summary to this path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = collect(args.run_root)

    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    if args.md_out is not None:
        args.md_out.parent.mkdir(parents=True, exist_ok=True)
        args.md_out.write_text(render_markdown(summary), encoding="utf-8")

    if args.json_out is None and args.md_out is None:
        print(json.dumps(summary, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
