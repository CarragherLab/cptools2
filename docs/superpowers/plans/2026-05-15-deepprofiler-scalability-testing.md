# DeepProfiler Scalability Testing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an evidence-driven Eddie test harness that evaluates whether DeepProfiler can scale beyond global serialization before cptools2 falls back to DINO feature extraction.

**Architecture:** Keep the production pipeline scheduler-friendly: one Nextflow task launches one Singularity container on one scheduler-assigned GPU. Add explicit test controls and evidence capture around DeepProfiler so we can compare global serialization, one-DeepProfiler-per-node isolation, TensorFlow memory-growth runtime changes, and selected full-node stress tests. DINO remains outside this plan except as the documented next decision branch if DeepProfiler routes fail.

**Tech Stack:** Nextflow DSL2, Eddie SGE, Singularity, TensorFlow/DeepProfiler, Python pytest, PowerShell-to-SSH operational commands.

---

## Current Evidence

- Generic GPU scheduling works with `queue=gpu` and `-l gpu=1`.
- `CUDA_VISIBLE_DEVICES` is now propagated into Singularity via `nextflow/conf/eddie.config`.
- Cellpose can run several independent one-GPU chunk jobs concurrently.
- DeepProfiler passes when serialized on H200.
- DeepProfiler fails when several `FEATURE_EXTRACT` tasks run concurrently on the same H200 node, even with distinct `CUDA_VISIBLE_DEVICES`.
- The failure signature is TensorFlow/cuDNN initialization:

```text
Could not create cudnn handle: CUDNN_STATUS_INTERNAL_ERROR
Failed to get convolution algorithm
```

The operational hypothesis is that a container image is not the limit. The same `.sif` can be launched many times across many nodes. The likely limit is the current DeepProfiler/TensorFlow runtime when multiple feature extraction processes initialize GPU/cuDNN work on the same physical node.

## Files And Responsibilities

- Modify `nextflow/conf/eddie.config`
  - Add opt-in DeepProfiler test environment variables and expose them to Singularity.
  - Keep production defaults unchanged: `CPTOOLS2_FEATURE_MAX_FORKS=1`.

- Modify `nextflow/modules/feature_extract.nf`
  - Add optional DeepProfiler runtime diagnostics.
  - Add optional TensorFlow memory-growth environment settings.
  - Add optional host-level DeepProfiler lock mode for one-active-feature-task-per-node testing.

- Create `scripts/eddie_deepprofiler_matrix.py`
  - Generate isolated run directories and route-specific YAML config variants for the DeepProfiler scalability matrix.
  - Set `chunk`, `max_chunks`, and `output_dir` in each generated YAML file so the run actually processes the intended workload.
  - Render Eddie commands that use the cptools2 CLI wrapper rather than bypassing YAML parsing.
  - Never hard-code private run roots into tracked files.

- Create `scripts/collect_deepprofiler_evidence.py`
  - Summarize Nextflow trace files, qacct data, GPU diagnostics, output counts, and first failure lines.
  - Emit one JSON and one Markdown report per run.

- Modify `tests/test_eddie_runtime_config.py`
  - Static tests for new environment controls, Singularity whitelist, and default serialization.

- Modify `tests/test_nextflow_architecture_smoke.py`
  - Static tests for feature extraction diagnostics, memory-growth settings, and host-lock guard.

- Create `docs/reference/deepprofiler-scalability.md`
  - Record the decision model, accepted evidence fields, and pass/fail criteria.

---

## Matrix To Evaluate

| Route | Purpose | Expected Outcome |
| --- | --- | --- |
| Baseline serialized | Preserve known-good behavior | Pass, establishes runtime and output baseline |
| Global concurrency 2 | Confirm current failure and collect clean evidence | Likely fail unless runtime flags help |
| Host-isolated concurrency | Allow multiple nodes but one DeepProfiler per node | Should pass if same-node co-location is the trigger |
| TensorFlow memory growth | Test whether TF memory preallocation causes the failure | Pass if cuDNN errors disappear |
| Memory growth plus host isolation | Conservative scalable route | Should pass if either mitigation helps |

Use generic GPU scheduling throughout:

```bash
CPTOOLS2_FEATURE_GPU_QUEUE=gpu
CPTOOLS2_FEATURE_GPU_RESOURCE="-l gpu=1"
```

Avoid A100 or H200 pinning unless the test objective is hardware-specific diagnostics.

---

## Task 1: Add Static Tests For Runtime Controls

**Files:**
- Modify: `tests/test_eddie_runtime_config.py`
- Modify: `tests/test_nextflow_architecture_smoke.py`

- [ ] **Step 1: Add failing Eddie config tests**

Append these tests to `tests/test_eddie_runtime_config.py`:

```python
def test_nextflow_eddie_config_exposes_deepprofiler_scalability_controls():
    text = EDDIE_CONFIG.read_text()

    assert "CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH" in text
    assert "CPTOOLS2_DEEPPROFILER_HOST_LOCK" in text
    assert "CPTOOLS2_DEEPPROFILER_LOCK_DIR" in text


def test_nextflow_eddie_config_whitelists_deepprofiler_scalability_controls():
    text = EDDIE_CONFIG.read_text()
    env_line = next(
        line for line in text.splitlines() if line.strip().startswith("envWhitelist")
    )

    assert "CUDA_VISIBLE_DEVICES" in env_line
    assert "CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH" in env_line
    assert "CPTOOLS2_DEEPPROFILER_HOST_LOCK" in env_line
    assert "CPTOOLS2_DEEPPROFILER_LOCK_DIR" in env_line
```

- [ ] **Step 2: Add failing feature module tests**

Append this test to `tests/test_nextflow_architecture_smoke.py`:

```python
def test_deepprofiler_feature_extract_has_scalability_guards():
    feature_extract = (
        ROOT / "nextflow" / "modules" / "feature_extract.nf"
    ).read_text()

    assert "TF_FORCE_GPU_ALLOW_GROWTH" in feature_extract
    assert "CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH" in feature_extract
    assert "CPTOOLS2_DEEPPROFILER_HOST_LOCK" in feature_extract
    assert "flock" in feature_extract
    assert "deepprofiler-host-lock" in feature_extract
```

- [ ] **Step 3: Run tests to verify failure**

Run:

```bash
pytest tests/test_eddie_runtime_config.py::test_nextflow_eddie_config_exposes_deepprofiler_scalability_controls tests/test_eddie_runtime_config.py::test_nextflow_eddie_config_whitelists_deepprofiler_scalability_controls tests/test_nextflow_architecture_smoke.py::test_deepprofiler_feature_extract_has_scalability_guards -q
```

Expected: all three new tests fail because the controls are not implemented.

- [ ] **Step 4: Commit failing tests**

```bash
git add tests/test_eddie_runtime_config.py tests/test_nextflow_architecture_smoke.py
git commit -m "test: define DeepProfiler scalability controls"
```

---

## Task 2: Add DeepProfiler Runtime Controls

**Files:**
- Modify: `nextflow/conf/eddie.config`
- Modify: `nextflow/modules/feature_extract.nf`

- [ ] **Step 1: Add environment controls to Eddie config**

In `nextflow/conf/eddie.config`, add these definitions after `cptools2ContainerRuntimeMode`:

```groovy
def cptools2DeepProfilerTfAllowGrowth = System.getenv('CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH') ?: 'false'
def cptools2DeepProfilerHostLock = System.getenv('CPTOOLS2_DEEPPROFILER_HOST_LOCK') ?: 'false'
def cptools2DeepProfilerLockDir = System.getenv('CPTOOLS2_DEEPPROFILER_LOCK_DIR') ?: "${cptools2ScratchRoot}/locks/deepprofiler"
```

Add these exports inside `beforeScript` after `CPTOOLS2_CONTAINER_RUNTIME_MODE`:

```bash
export CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH="${cptools2DeepProfilerTfAllowGrowth}"
export CPTOOLS2_DEEPPROFILER_HOST_LOCK="${cptools2DeepProfilerHostLock}"
export CPTOOLS2_DEEPPROFILER_LOCK_DIR="${cptools2DeepProfilerLockDir}"
mkdir -p "$CPTOOLS2_DEEPPROFILER_LOCK_DIR"
```

Extend `singularity.envWhitelist` with:

```text
CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH,CPTOOLS2_DEEPPROFILER_HOST_LOCK,CPTOOLS2_DEEPPROFILER_LOCK_DIR
```

- [ ] **Step 2: Add a DeepProfiler launcher block in `FEATURE_EXTRACT`**

In `nextflow/modules/feature_extract.nf`, replace the current DeepProfiler command:

```bash
python -m deepprofiler \\
    --root dp_project/ \\
    --config config.json \\
    --metadata index.csv \\
    --exp cell_painting \\
    --gpu 0 \\
    profile
```

with:

```bash
cat > run_deepprofiler.py <<'PY'
import os
import subprocess
import sys

allow_growth = os.environ.get("CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH", "false").lower()

if allow_growth in {"1", "true", "yes"}:
    os.environ["TF_FORCE_GPU_ALLOW_GROWTH"] = "true"

print("CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH=" + allow_growth, flush=True)
print("TF_FORCE_GPU_ALLOW_GROWTH=" + os.environ.get("TF_FORCE_GPU_ALLOW_GROWTH", "unset"), flush=True)

cmd = [
    sys.executable,
    "-m",
    "deepprofiler",
    "--root",
    "dp_project/",
    "--config",
    "config.json",
    "--metadata",
    "index.csv",
    "--exp",
    "cell_painting",
    "--gpu",
    "0",
    "profile",
]
raise SystemExit(subprocess.call(cmd))
PY

run_deepprofiler_command() {
    python run_deepprofiler.py
}

if [ "${CPTOOLS2_DEEPPROFILER_HOST_LOCK:-false}" = "true" ]; then
    lock_dir="${CPTOOLS2_DEEPPROFILER_LOCK_DIR:-${PWD}/deepprofiler-host-lock}"
    mkdir -p "$lock_dir"
    host_name="$(hostname | cut -d. -f1)"
    lock_file="$lock_dir/${host_name}.lock"
    echo "DeepProfiler host lock enabled: $lock_file"
    flock "$lock_file" python run_deepprofiler.py
else
    run_deepprofiler_command
fi
```

- [ ] **Step 3: Run focused static tests**

Run:

```bash
pytest tests/test_eddie_runtime_config.py::test_nextflow_eddie_config_exposes_deepprofiler_scalability_controls tests/test_eddie_runtime_config.py::test_nextflow_eddie_config_whitelists_deepprofiler_scalability_controls tests/test_nextflow_architecture_smoke.py::test_deepprofiler_feature_extract_has_scalability_guards -q
```

Expected: all three tests pass.

- [ ] **Step 4: Run existing runtime config tests**

Run:

```bash
pytest tests/test_eddie_runtime_config.py tests/test_nextflow_architecture_smoke.py -q
```

Expected: pass.

- [ ] **Step 5: Commit runtime controls**

```bash
git add nextflow/conf/eddie.config nextflow/modules/feature_extract.nf
git commit -m "feat: add DeepProfiler scalability runtime controls"
```

---

## Task 3: Add Matrix Runner

**Files:**
- Create: `scripts/eddie_deepprofiler_matrix.py`
- Test: `tests/test_deepprofiler_matrix_runner.py`

- [ ] **Step 1: Add tests for matrix generation**

Create `tests/test_deepprofiler_matrix_runner.py`:

```python
from scripts import eddie_deepprofiler_matrix


def test_matrix_contains_serialized_host_lock_and_memory_growth_routes():
    matrix = eddie_deepprofiler_matrix.build_matrix(max_chunks=5, chunk=24)
    names = {entry["name"] for entry in matrix}

    assert "dp-serialized" in names
    assert "dp-concurrency2" in names
    assert "dp-hostlock-concurrency4" in names
    assert "dp-growth-concurrency2" in names
    assert "dp-growth-hostlock-concurrency4" in names


def test_matrix_entries_use_generic_gpu_queue_and_one_gpu_resource():
    matrix = eddie_deepprofiler_matrix.build_matrix(max_chunks=5, chunk=24)

    for entry in matrix:
        env = entry["env"]
        assert env["CPTOOLS2_FEATURE_GPU_QUEUE"] == "gpu"
        assert env["CPTOOLS2_FEATURE_GPU_RESOURCE"] == "-l gpu=1"
        assert env["CPTOOLS2_GPU_RESOURCE"] == "-l gpu=1"


def test_render_remote_command_exports_expected_environment():
    entry = eddie_deepprofiler_matrix.build_matrix(max_chunks=5, chunk=24)[0]
    command = eddie_deepprofiler_matrix.render_remote_command(
        entry,
        project_root="/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2",
        scratch_root="/exports/eddie/scratch/mharvey2/cptools2-ai-update",
        config_path="config/loop440-eddie-smoke.yaml",
    )

    assert "export CPTOOLS2_FEATURE_MAX_FORKS=" in command
    assert "export CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH=" in command
    assert "python3 -m cptools2.__main__ pipeline" in command
    assert "route_config.yml" in command


def test_route_config_sets_chunk_max_chunks_and_output_dir(tmp_path):
    base = tmp_path / "base.yml"
    base.write_text(
        "input_dir: /input\n"
        "output_dir: /old-output\n"
        "plates:\n"
        "  - tiny-plate-001\n"
        "chunk: 96\n"
        "max_chunks: 1\n"
    )
    entry = eddie_deepprofiler_matrix.build_matrix(max_chunks=5, chunk=24)[0]
    route_dir = tmp_path / "route"

    config_path = eddie_deepprofiler_matrix.write_route_config(
        entry,
        base_config=base,
        route_dir=route_dir,
    )

    text = config_path.read_text()
    assert "chunk: 24" in text
    assert "max_chunks: 5" in text
    assert f"output_dir: {route_dir.as_posix()}/outputs" in text
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest tests/test_deepprofiler_matrix_runner.py -q
```

Expected: fail because `scripts/eddie_deepprofiler_matrix.py` does not exist.

- [ ] **Step 3: Implement matrix runner**

Create `scripts/eddie_deepprofiler_matrix.py`:

```python
from __future__ import annotations

import argparse
import json
import shlex
from pathlib import Path

import yaml


def build_matrix(max_chunks: int, chunk: int) -> list[dict[str, object]]:
    base_env = {
        "CPTOOLS2_GPU_QUEUE": "gpu",
        "CPTOOLS2_GPU_RESOURCE": "-l gpu=1",
        "CPTOOLS2_FEATURE_GPU_QUEUE": "gpu",
        "CPTOOLS2_FEATURE_GPU_RESOURCE": "-l gpu=1",
        "CPTOOLS2_SEGMENT_GPU_QUEUE": "gpu",
        "CPTOOLS2_SEGMENT_GPU_RESOURCE": "-l gpu=1",
        "CPTOOLS2_SEGMENT_MAX_FORKS": "5",
        "CPTOOLS2_DEEPPROFILER_LOCK_DIR": "${CPTOOLS2_SCRATCH_ROOT}/locks/deepprofiler",
    }

    routes = [
        ("dp-serialized", 1, "false", "false"),
        ("dp-concurrency2", 2, "false", "false"),
        ("dp-hostlock-concurrency4", 4, "false", "true"),
        ("dp-growth-concurrency2", 2, "true", "false"),
        ("dp-growth-hostlock-concurrency4", 4, "true", "true"),
    ]

    matrix = []
    for name, feature_forks, growth, host_lock in routes:
        env = dict(base_env)
        env.update(
            {
                "CPTOOLS2_FEATURE_MAX_FORKS": str(feature_forks),
                "CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH": growth,
                "CPTOOLS2_DEEPPROFILER_HOST_LOCK": host_lock,
            }
        )
        matrix.append(
            {
                "name": name,
                "chunk": chunk,
                "max_chunks": max_chunks,
                "env": env,
            }
        )
    return matrix


def _export_lines(env: dict[str, str]) -> list[str]:
    return [f"export {key}={shlex.quote(value)}" for key, value in sorted(env.items())]


def write_route_config(
    entry: dict[str, object],
    *,
    base_config: Path,
    route_dir: Path,
) -> Path:
    config = yaml.safe_load(base_config.read_text())
    config["chunk"] = int(entry["chunk"])
    config["max_chunks"] = int(entry["max_chunks"])
    config["output_dir"] = f"{route_dir.as_posix()}/outputs"
    route_dir.mkdir(parents=True, exist_ok=True)
    out = route_dir / "route_config.yml"
    out.write_text(yaml.safe_dump(config, sort_keys=False))
    return out


def render_remote_command(
    entry: dict[str, object],
    *,
    project_root: str,
    scratch_root: str,
    config_path: str,
) -> str:
    name = str(entry["name"])
    env = dict(entry["env"])
    env["CPTOOLS2_PROJECT_ROOT"] = project_root
    env["CPTOOLS2_SCRATCH_ROOT"] = scratch_root
    env["CPTOOLS2_MATRIX_RUN_NAME"] = name

    run_root = f"{scratch_root}/diagnostics/deepprofiler-scalability/{name}"
    route_config = f"{run_root}/route_config.yml"
    lines = [
        "set -euo pipefail",
        f"cd {shlex.quote(project_root)}",
        f"mkdir -p {shlex.quote(run_root)}",
        *_export_lines(env),
        ". config/eddie_env.sh",
        (
            "python3 -m cptools2.__main__ pipeline "
            f"{shlex.quote(route_config)}"
        ),
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk", type=int, default=24)
    parser.add_argument("--max-chunks", type=int, default=5)
    parser.add_argument("--project-root", required=True)
    parser.add_argument("--scratch-root", required=True)
    parser.add_argument("--config-path", default="config/loop440-eddie-smoke.yaml")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    matrix = build_matrix(max_chunks=args.max_chunks, chunk=args.chunk)
    rendered = []
    for entry in matrix:
        route_dir = Path(args.scratch_root) / "diagnostics" / "deepprofiler-scalability" / str(entry["name"])
        route_config = write_route_config(
            entry,
            base_config=Path(args.config_path),
            route_dir=route_dir,
        )
        rendered.append(
            {
                **entry,
                "run_root": str(route_dir),
                "route_config": str(route_config),
                "output_root": str(route_dir / "outputs"),
                "remote_command": render_remote_command(
                    entry,
                    project_root=args.project_root,
                    scratch_root=args.scratch_root,
                    config_path=args.config_path,
                ),
            }
        )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(rendered, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run matrix runner tests**

Run:

```bash
pytest tests/test_deepprofiler_matrix_runner.py -q
```

Expected: pass.

- [ ] **Step 5: Commit matrix runner**

```bash
git add scripts/eddie_deepprofiler_matrix.py tests/test_deepprofiler_matrix_runner.py
git commit -m "feat: add DeepProfiler scalability matrix runner"
```

---

## Task 4: Add Evidence Collector

**Files:**
- Create: `scripts/collect_deepprofiler_evidence.py`
- Test: `tests/test_collect_deepprofiler_evidence.py`

- [ ] **Step 1: Add collector tests**

Create `tests/test_collect_deepprofiler_evidence.py`:

```python
from pathlib import Path

from scripts import collect_deepprofiler_evidence


def test_collect_trace_summary_counts_feature_statuses(tmp_path):
    trace = tmp_path / "trace.txt"
    trace.write_text(
        "\t".join(["task_id", "process", "status", "exit", "realtime", "peak_rss"]) + "\n"
        + "\t".join(["1", "FEATURE_EXTRACT", "COMPLETED", "0", "1m 2s", "160 GB"]) + "\n"
        + "\t".join(["2", "FEATURE_EXTRACT", "FAILED", "1", "10s", "4 GB"]) + "\n"
    )

    summary = collect_deepprofiler_evidence.collect_trace_summary(trace)

    assert summary["feature_total"] == 2
    assert summary["feature_completed"] == 1
    assert summary["feature_failed"] == 1


def test_first_error_line_finds_cudnn_signature(tmp_path):
    log = tmp_path / ".command.err"
    log.write_text(
        "noise\n"
        "Could not create cudnn handle: CUDNN_STATUS_INTERNAL_ERROR\n"
        "more noise\n"
    )

    assert collect_deepprofiler_evidence.first_error_line(tmp_path) == (
        "Could not create cudnn handle: CUDNN_STATUS_INTERNAL_ERROR"
    )


def test_output_counts_detect_npz_and_masks(tmp_path):
    (tmp_path / "features").mkdir()
    (tmp_path / "features" / "a.npz").write_text("x")
    (tmp_path / "masks").mkdir()
    (tmp_path / "masks" / "mask.tiff").write_text("x")

    counts = collect_deepprofiler_evidence.output_counts(tmp_path)

    assert counts["npz"] == 1
    assert counts["masks"] == 1
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest tests/test_collect_deepprofiler_evidence.py -q
```

Expected: fail because `scripts/collect_deepprofiler_evidence.py` does not exist.

- [ ] **Step 3: Implement evidence collector**

Create `scripts/collect_deepprofiler_evidence.py`:

```python
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


def collect_trace_summary(trace_path: Path) -> dict[str, int]:
    summary = {
        "feature_total": 0,
        "feature_completed": 0,
        "feature_failed": 0,
        "cellpose_total": 0,
        "cellpose_completed": 0,
        "cellpose_failed": 0,
    }
    if not trace_path.exists():
        return summary

    with trace_path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            process = row.get("process", "")
            status = row.get("status", "")
            if "FEATURE_EXTRACT" in process:
                summary["feature_total"] += 1
                if status == "COMPLETED":
                    summary["feature_completed"] += 1
                elif status == "FAILED":
                    summary["feature_failed"] += 1
            if "CELLPOSE_SEGMENT" in process:
                summary["cellpose_total"] += 1
                if status == "COMPLETED":
                    summary["cellpose_completed"] += 1
                elif status == "FAILED":
                    summary["cellpose_failed"] += 1
    return summary


def first_error_line(run_root: Path) -> str:
    for path in run_root.rglob("*"):
        if path.is_file() and path.name in {".command.err", ".command.log", "launcher.log"}:
            try:
                lines = path.read_text(errors="replace").splitlines()
            except OSError:
                continue
            for line in lines:
                if any(pattern in line for pattern in ERROR_PATTERNS):
                    return line.strip()
    return ""


def output_counts(run_root: Path) -> dict[str, int]:
    return {
        "npz": sum(1 for _ in run_root.rglob("*.npz")),
        "masks": sum(
            1
            for path in run_root.rglob("*")
            if path.suffix.lower() in {".tif", ".tiff"} and "mask" in path.name.lower()
        ),
        "no_cells": sum(1 for path in run_root.rglob("no_cells.tsv") if path.is_file()),
    }


def collect(run_root: Path) -> dict[str, object]:
    trace_path = run_root / "trace.txt"
    return {
        "run_root": str(run_root),
        "trace": collect_trace_summary(trace_path),
        "outputs": output_counts(run_root),
        "first_error": first_error_line(run_root),
    }


def render_markdown(summary: dict[str, object]) -> str:
    trace = summary["trace"]
    outputs = summary["outputs"]
    return "\n".join(
        [
            "# DeepProfiler Scalability Evidence",
            "",
            f"Run root: `{summary['run_root']}`",
            "",
            "## Trace",
            "",
            f"- Feature tasks: {trace['feature_completed']}/{trace['feature_total']} completed",
            f"- Feature failures: {trace['feature_failed']}",
            f"- Cellpose tasks: {trace['cellpose_completed']}/{trace['cellpose_total']} completed",
            f"- Cellpose failures: {trace['cellpose_failed']}",
            "",
            "## Outputs",
            "",
            f"- `.npz` files: {outputs['npz']}",
            f"- mask files: {outputs['masks']}",
            f"- no-cell markers: {outputs['no_cells']}",
            "",
            "## First Error",
            "",
            summary["first_error"] or "None detected",
            "",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_root", type=Path)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--md-out", type=Path)
    args = parser.parse_args()

    summary = collect(args.run_root)
    if args.json_out:
        args.json_out.write_text(json.dumps(summary, indent=2) + "\n")
    if args.md_out:
        args.md_out.write_text(render_markdown(summary))
    if not args.json_out and not args.md_out:
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run collector tests**

Run:

```bash
pytest tests/test_collect_deepprofiler_evidence.py -q
```

Expected: pass.

- [ ] **Step 5: Commit evidence collector**

```bash
git add scripts/collect_deepprofiler_evidence.py tests/test_collect_deepprofiler_evidence.py
git commit -m "feat: collect DeepProfiler scalability evidence"
```

---

## Task 5: Document The Decision Rules

**Files:**
- Create: `docs/reference/deepprofiler-scalability.md`

- [ ] **Step 1: Write reference documentation**

Create `docs/reference/deepprofiler-scalability.md`:

```markdown
# DeepProfiler Scalability On Eddie

## Default Policy

DeepProfiler feature extraction defaults to global serialization:

```bash
CPTOOLS2_FEATURE_MAX_FORKS=1
```

This is the only production-safe policy until a scalability route passes the
matrix in this document.

## Why Serialization Exists

DeepProfiler currently runs through an older TensorFlow/CUDA stack. Eddie tests
showed serialized DeepProfiler runs complete on H200, while same-node concurrent
DeepProfiler runs can fail during cuDNN initialization:

```text
Could not create cudnn handle: CUDNN_STATUS_INTERNAL_ERROR
Failed to get convolution algorithm
```

The container image can be launched many times. The observed limit is concurrent
DeepProfiler/TensorFlow GPU processes on the same physical node.

## Routes Under Test

| Route | Environment |
| --- | --- |
| Serialized baseline | `CPTOOLS2_FEATURE_MAX_FORKS=1` |
| Global concurrency | `CPTOOLS2_FEATURE_MAX_FORKS=2` or `4` |
| Host-isolated concurrency | `CPTOOLS2_FEATURE_MAX_FORKS=4`, `CPTOOLS2_DEEPPROFILER_HOST_LOCK=true` |
| TensorFlow memory growth | `CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH=true` |
| Combined mitigation | host lock plus TensorFlow memory growth |

All routes use generic GPU scheduling:

```bash
CPTOOLS2_FEATURE_GPU_QUEUE=gpu
CPTOOLS2_FEATURE_GPU_RESOURCE="-l gpu=1"
```

## Pass Criteria

A route passes only if all conditions hold:

- Nextflow exits with status `0`.
- All expected `FEATURE_EXTRACT` tasks complete.
- DeepProfiler publishes the expected `.npz` count for the tested chunks.
- No cuDNN, convolution algorithm, bus error, or unset CUDA visibility errors appear.
- qacct shows exit status `0` for feature extraction tasks.
- GPU diagnostics show scheduler-assigned `CUDA_VISIBLE_DEVICES`.

## Decision

If host-isolated concurrency passes and global concurrency fails, the next full
test plate should use one active DeepProfiler task per physical node.

If TensorFlow memory growth makes same-node concurrency pass, repeat the test at
larger chunk count before changing production defaults.

If all DeepProfiler scalability routes fail, keep DeepProfiler serialized for
compatibility runs and evaluate DINO feature extraction as the scalable path.
```

- [ ] **Step 2: Commit documentation**

```bash
git add docs/reference/deepprofiler-scalability.md
git commit -m "docs: define DeepProfiler scalability decision rules"
```

---

## Task 6: Deploy And Run Eddie Matrix

**Files:**
- Remote deployment only, no tracked source changes expected.

- [ ] **Step 1: Run local focused tests**

Run:

```bash
pytest tests/test_eddie_runtime_config.py tests/test_nextflow_architecture_smoke.py tests/test_deepprofiler_matrix_runner.py tests/test_collect_deepprofiler_evidence.py -q
```

Expected: pass.

- [ ] **Step 2: Copy updated files to Eddie**

Run from Windows PowerShell:

```powershell
scp nextflow/conf/eddie.config nextflow/modules/feature_extract.nf scripts/eddie_deepprofiler_matrix.py scripts/collect_deepprofiler_evidence.py mharvey2@eddie.ecdf.ed.ac.uk:/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2/
```

Expected: files copy without error. If directory layout requires subdirectories, copy each file to its matching remote path:

```powershell
scp nextflow/conf/eddie.config mharvey2@eddie.ecdf.ed.ac.uk:/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2/nextflow/conf/eddie.config
scp nextflow/modules/feature_extract.nf mharvey2@eddie.ecdf.ed.ac.uk:/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2/nextflow/modules/feature_extract.nf
scp scripts/eddie_deepprofiler_matrix.py mharvey2@eddie.ecdf.ed.ac.uk:/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2/scripts/eddie_deepprofiler_matrix.py
scp scripts/collect_deepprofiler_evidence.py mharvey2@eddie.ecdf.ed.ac.uk:/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2/scripts/collect_deepprofiler_evidence.py
```

- [ ] **Step 3: Generate matrix commands on Eddie**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 && python3 scripts/eddie_deepprofiler_matrix.py --chunk 24 --max-chunks 5 --project-root /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 --scratch-root /exports/eddie/scratch/mharvey2/cptools2-ai-update --config-path config/loop440-eddie-smoke.yaml --out /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-scalability/matrix.json"
```

Expected: `matrix.json` exists and contains five route entries.

- [ ] **Step 4: Run routes in this order**

Run one route at a time, waiting for completion before the next route:

```text
1. dp-serialized
2. dp-concurrency2
3. dp-hostlock-concurrency4
4. dp-growth-concurrency2
5. dp-growth-hostlock-concurrency4
```

Operational command pattern:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 && python3 - <<'PY'
import json
import subprocess
from pathlib import Path

matrix = json.loads(Path('/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-scalability/matrix.json').read_text())
route = next(item for item in matrix if item['name'] == 'dp-serialized')
subprocess.run(['bash', '-lc', route['remote_command']], check=False)
PY"
```

Expected for `dp-serialized`: status `0`, expected feature files, no cuDNN error.

Expected for `dp-concurrency2`: either pass with useful evidence or fail with captured cuDNN error. A fail here is informative.

Expected for host-lock routes: no simultaneous same-host DeepProfiler command execution in task logs.

- [ ] **Step 5: Monitor queue while each route runs**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "qstat -u mharvey2 | egrep 'nf-|cptools2|FEATURE|CELLPOSE|Nextflow' || true"
```

Expected: feature jobs are in `gpu`, not `staging`, and request `gpu=1`.

- [ ] **Step 6: Collect evidence after each route**

Run for each route:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 && python3 scripts/collect_deepprofiler_evidence.py /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-scalability/dp-serialized --json-out /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-scalability/dp-serialized/evidence.json --md-out /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-scalability/dp-serialized/evidence.md"
```

Expected: `evidence.json` and `evidence.md` summarize trace status, output counts, and first error.

---

## Task 7: Decide Full Test Plate Policy

**Files:**
- Modify: `docs/reference/deepprofiler-scalability.md`
- Modify: `docs/superpowers/specs/2026-05-14-gpu-mig-chunk-strategy-design.md`

- [ ] **Step 1: Classify each route**

Use this classification table format and replace the example values with the
observed counts from each `evidence.json` file:

```markdown
| Route | Status | Feature tasks | `.npz` count | First error | Decision |
| --- | --- | ---: | ---: | --- | --- |
| dp-serialized | pass | 5/5 | 360 | none | baseline |
| dp-concurrency2 | fail | 1/5 | 72 | Could not create cudnn handle | reject |
| dp-hostlock-concurrency4 | pass | 5/5 | 360 | none | accept |
| dp-growth-concurrency2 | fail | 1/5 | 72 | Failed to get convolution algorithm | reject |
| dp-growth-hostlock-concurrency4 | pass | 5/5 | 360 | none | accept |
```

- [ ] **Step 2: Apply decision rules**

Use these rules exactly:

```text
If dp-concurrency2 passes and dp-growth-concurrency2 passes:
  choose TensorFlow memory growth route for a chunk 48 repeat.

If dp-concurrency2 fails and dp-hostlock-concurrency4 passes:
  choose one-active-DeepProfiler-per-node for the full test plate.

If only dp-serialized passes:
  run the first full test plate with CPTOOLS2_FEATURE_MAX_FORKS=1.

If every DeepProfiler route fails:
  stop DeepProfiler scalability work and open the DINO evaluation plan.
```

- [ ] **Step 3: Update docs with the selected policy**

Add one of these sections to `docs/reference/deepprofiler-scalability.md`,
matching the decision from Step 2.

Use this section if host isolation is selected:

```markdown
## Selected Policy For Full Test Plate

Route: `dp-hostlock-concurrency4`

Environment:

```bash
CPTOOLS2_FEATURE_GPU_QUEUE=gpu
CPTOOLS2_FEATURE_GPU_RESOURCE="-l gpu=1"
CPTOOLS2_FEATURE_MAX_FORKS=4
CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH=false
CPTOOLS2_DEEPPROFILER_HOST_LOCK=true
```

Reason:

The host-lock route completed all feature extraction tasks and produced the
expected `.npz` count while unrestricted same-node concurrency failed with
cuDNN initialization errors. This supports running multiple DeepProfiler chunks
across the cluster while preventing more than one active DeepProfiler process on
the same physical node.
```

Use this section if TensorFlow memory growth is selected:

```markdown
## Selected Policy For Full Test Plate

Route: `dp-growth-concurrency2`

Environment:

```bash
CPTOOLS2_FEATURE_GPU_QUEUE=gpu
CPTOOLS2_FEATURE_GPU_RESOURCE="-l gpu=1"
CPTOOLS2_FEATURE_MAX_FORKS=2
CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH=true
CPTOOLS2_DEEPPROFILER_HOST_LOCK=false
```

Reason:

The TensorFlow memory-growth route completed all feature extraction tasks and
produced the expected `.npz` count without cuDNN initialization errors. This
supports a controlled repeat at `chunk: 48` before raising concurrency for a
full test plate.
```

Use this section if only serialization is selected:

```markdown
## Selected Policy For Full Test Plate

Route: `dp-serialized`

Environment:

```bash
CPTOOLS2_FEATURE_GPU_QUEUE=gpu
CPTOOLS2_FEATURE_GPU_RESOURCE="-l gpu=1"
CPTOOLS2_FEATURE_MAX_FORKS=1
CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH=false
CPTOOLS2_DEEPPROFILER_HOST_LOCK=false
```

Reason:

Only the serialized DeepProfiler route completed all feature extraction tasks
and produced the expected `.npz` count. The full test plate should prioritize
correctness and output completeness before further feature-extraction
parallelism work.
```

- [ ] **Step 4: Commit policy update**

```bash
git add docs/reference/deepprofiler-scalability.md docs/superpowers/specs/2026-05-14-gpu-mig-chunk-strategy-design.md
git commit -m "docs: record DeepProfiler scalability policy"
```

---

## Task 8: Full Test Plate Gate

**Files:**
- Remote run only unless config defaults need to change after review.

- [ ] **Step 1: Prepare full test plate config**

Use the selected DeepProfiler route from Task 7. Keep generic GPU scheduling. Start with the chunk size that passed the previous matrix most cleanly:

```text
Preferred order:
1. chunk 48
2. chunk 24
3. chunk 96 only if smaller chunks are too slow and memory is clean
```

- [ ] **Step 2: Submit the full test plate**

Create a full-plate route config first so `chunk`, `max_chunks`, and `output_dir`
are explicit:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 && python3 - <<'PY'
from pathlib import Path
import yaml

base = Path('config/loop440-eddie-smoke.yaml')
out_dir = Path('/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/full-test-plate')
config = yaml.safe_load(base.read_text())
config['chunk'] = 48
config.pop('max_chunks', None)
config['output_dir'] = f'{out_dir}/outputs'
out_dir.mkdir(parents=True, exist_ok=True)
(out_dir / 'route_config.yml').write_text(yaml.safe_dump(config, sort_keys=False))
PY"
```

Use the same selected environment controls to submit through the cptools2 CLI:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 && . config/eddie_env.sh && export CPTOOLS2_GPU_QUEUE=gpu CPTOOLS2_GPU_RESOURCE='-l gpu=1' CPTOOLS2_FEATURE_GPU_QUEUE=gpu CPTOOLS2_FEATURE_GPU_RESOURCE='-l gpu=1' CPTOOLS2_FEATURE_MAX_FORKS=1 && python3 -m cptools2.__main__ pipeline /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/full-test-plate/route_config.yml"
```

If Task 7 selects host lock or memory growth, add:

```bash
export CPTOOLS2_DEEPPROFILER_HOST_LOCK=true
export CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH=true
```

only for the selected route.

- [ ] **Step 3: Collect full-plate evidence**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 && python3 scripts/collect_deepprofiler_evidence.py /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/full-test-plate --json-out /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/full-test-plate/evidence.json --md-out /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/full-test-plate/evidence.md"
```

Expected: full plate exits `0`, output counts match expected chunks, no cuDNN errors.

---

## Task 9: DINO Fallback Gate

**Files:**
- No source changes unless DeepProfiler scalability fails.

- [ ] **Step 1: Trigger fallback only under explicit failure conditions**

Open DINO evaluation only if one of these is true:

```text
1. Host-isolated DeepProfiler fails.
2. TensorFlow memory-growth DeepProfiler fails.
3. Serialized DeepProfiler full test plate is too slow for production use.
4. DeepProfiler requires GPU pinning that undermines scheduler acceptance.
```

- [ ] **Step 2: Use the existing DINO plan**

Use:

```text
docs/superpowers/plans/2026-05-14-dinov2-phase-feature-extraction-implementation.md
```

as the starting point. The DINO evaluation must include biological output review, because DINO changes the feature representation and is not just a runtime substitution.

---

## Self-Review

**Spec coverage:** The plan covers all agreed DeepProfiler approaches: baseline serialization, generic concurrency, one-per-node isolation, TensorFlow memory growth, combined mitigation, full test plate gate, and DINO fallback.

**Placeholder scan:** No task uses open placeholders for implementation. The policy section includes concrete selectable text for each possible decision branch.

**Type consistency:** Environment variable names are consistent across tests, Eddie config, feature module, matrix runner, evidence collector, and documentation.

**Scope check:** This plan is scoped to DeepProfiler scalability testing. DINO is deliberately a fallback gate, not mixed into the same implementation.
