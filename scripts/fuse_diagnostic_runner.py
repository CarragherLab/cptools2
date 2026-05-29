"""Generate repo-safe Phase 2.9 Singularity/FUSE diagnostic runs.

The generator writes a scratch-only cptools2 config plus a launcher script. It
does not submit SGE jobs; use ``--launch`` only when deliberately starting the
diagnostic from an Eddie shell.
"""

from __future__ import annotations

import argparse
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Mapping, Optional, Sequence

import yaml

DEFAULT_PLATE_ID = "diagnostic-plate"
DEFAULT_PLATE_SIZE_GB = 103
DEFAULT_CHUNK_SIZE = 50
DEFAULT_MAX_CHUNKS = 1
DEFAULT_SCRATCH_QUOTA_GB = 2000
DEFAULT_STAGES = ["segment"]
VALID_CONTAINER_RUNTIME_MODES = {
    "baseline",
    "node-local",
    "node_local",
    "unsquash",
    "scratch-sif",
}
CHANNELS = ["DNA", "RNA", "ER", "AGP", "Mito"]
EXPECTED_CHANNELS = [1, 2, 3, 4, 5]


@dataclass(frozen=True)
class DiagnosticPaths:
    run_id: str
    run_dir: Path
    config_path: Path
    launch_path: Path
    metadata_path: Path
    log_dir: Path
    status_file: Path
    log_file: Path
    staged_root: Path


def _env(name: str, environ: Mapping[str, str]) -> str:
    value = environ.get(name)
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _default_diag_root(environ: Mapping[str, str]) -> Path:
    scratch_root = _env("CPTOOLS2_SCRATCH_ROOT", environ)
    return Path(scratch_root) / "diagnostics" / "fuse"


def _run_id(variant: str, chunk_size: int, now: Optional[datetime] = None) -> str:
    timestamp = (now or datetime.now()).strftime("%Y%m%d-%H%M%S")
    return f"fuse-{variant}-chunk{chunk_size}-{timestamp}"


def _build_paths(
    variant: str,
    chunk_size: int,
    diag_root: Path,
    now: Optional[datetime] = None,
) -> DiagnosticPaths:
    run_id = _run_id(variant, chunk_size, now=now)
    run_dir = diag_root / run_id
    log_dir = run_dir / "logs"
    return DiagnosticPaths(
        run_id=run_id,
        run_dir=run_dir,
        config_path=run_dir / "config.yaml",
        launch_path=run_dir / "launch.sh",
        metadata_path=run_dir / "metadata.txt",
        log_dir=log_dir,
        status_file=log_dir / "status.txt",
        log_file=log_dir / "launcher.log",
        staged_root=run_dir / "staged-root",
    )


def build_config(
    paths: DiagnosticPaths,
    plate_id: str,
    chunk_size: int,
    container_dir: str,
    stages: Optional[Sequence[str]] = None,
    plate_size_gb: int = DEFAULT_PLATE_SIZE_GB,
    max_chunks: int = DEFAULT_MAX_CHUNKS,
    scratch_quota_gb: int = DEFAULT_SCRATCH_QUOTA_GB,
) -> dict:
    """Return the scratch-only diagnostic config."""

    selected_stages = list(stages or DEFAULT_STAGES)

    return {
        "input_dir": str(paths.staged_root),
        "output_dir": str(paths.run_dir / "results"),
        "plates": [plate_id],
        "plate_sizes_gb": {plate_id: plate_size_gb},
        "stage_data": False,
        "max_chunks": max_chunks,
        "chunk": chunk_size,
        "scratch_quota_gb": scratch_quota_gb,
        "scratch_utilisation_fraction": 0.75,
        "scratch_work_factor": 1.3,
        "container_path": container_dir,
        "illum_pipeline_calculate": (
            "${CPTOOLS2_PROJECT_ROOT}/cptools2/templates/illum_calculate.cppipe"
        ),
        "illum_pipeline_apply": (
            "${CPTOOLS2_PROJECT_ROOT}/cptools2/templates/illum_apply.cppipe"
        ),
        "seg_pipeline": (
            "${CPTOOLS2_PROJECT_ROOT}/cptools2/templates/nuclear_segmentation.cppipe"
        ),
        "pipeline": (
            "${CPTOOLS2_PROJECT_ROOT}/cptools2/templates/nuclear_segmentation.cppipe"
        ),
        "commands location": str(paths.run_dir / "commands"),
        "channels": CHANNELS,
        "expected_channels": EXPECTED_CHANNELS,
        "stages": selected_stages,
        "segmentation": {
            "engine": "cellpose",
            "model": "cpsam",
            "channel": 1,
            "batch_size": 1,
        },
        "feature_extraction": {
            "tool": "deepprofiler",
            "config": (
                "${CPTOOLS2_PROJECT_ROOT}/cptools2/templates/"
                "deepprofiler_config.json"
            ),
            "weights": (
                "${CPTOOLS2_MODEL_DIR}/deepprofiler/"
                "Cell_Painting_CNN_v1.hdf5"
            ),
            "batch_size": 8,
        },
    }


def _runtime_mode(environ: Mapping[str, str]) -> str:
    mode = environ.get("CPTOOLS2_CONTAINER_RUNTIME_MODE") or "baseline"
    if mode not in VALID_CONTAINER_RUNTIME_MODES:
        expected = ", ".join(sorted(VALID_CONTAINER_RUNTIME_MODES))
        raise ValueError(
            f"Unsupported CPTOOLS2_CONTAINER_RUNTIME_MODE: {mode}. "
            f"Expected one of: {expected}"
        )
    return mode


def build_launcher() -> str:
    """Return a launcher that records an exit status for every failure path."""

    return """#!/usr/bin/env bash
set -uo pipefail

project_root="${1:?project root required}"
config_path="${2:?config path required}"
status_file="${3:?status file required}"
log_file="${4:?log file required}"
temp_mode="${5:-scratch}"
container_dir="${6:?container dir required}"

mkdir -p "$(dirname "$status_file")" "$(dirname "$log_file")"
rm -f "$status_file"
(
    cd "$project_root" || exit 11
    . config/eddie_env.sh || exit 12
    export CPTOOLS2_CONTAINER_DIR="$container_dir"
    run_dir="$(dirname "$config_path")"
    mkdir -p "$run_dir" || exit 13
    export NXF_HOME="$run_dir/.nxf_home"
    mkdir -p "$NXF_HOME" || exit 14
    if [ "$temp_mode" = "node-local" ]; then
        export CPTOOLS2_WORK_ROOT="${TMPDIR:-/tmp}/cptools2-work-${USER}"
        export SINGULARITY_TMPDIR="${TMPDIR:-/tmp}/cptools2-singularity-tmp-${USER}"
        export SINGULARITY_CACHEDIR="${TMPDIR:-/tmp}/cptools2-singularity-cache-${USER}"
        mkdir -p \\
            "$CPTOOLS2_WORK_ROOT" \\
            "$SINGULARITY_TMPDIR" \\
            "$SINGULARITY_CACHEDIR" || exit 15
    fi
    cd "$run_dir" || exit 16
    PYTHONPATH="$project_root" \\
        python3 -m cptools2.__main__ pipeline "$config_path" --resume
    rc=$?
    printf "%s\\n" "$rc" > "$status_file"
    exit "$rc"
) > "$log_file" 2>&1
rc=$?
if [ ! -f "$status_file" ]; then
    printf "%s\\n" "$rc" > "$status_file"
fi
exit "$rc"
"""


def _write_metadata(
    paths: DiagnosticPaths,
    variant: str,
    chunk_size: int,
    container_dir: str,
    temp_mode: str,
    source_staged: str,
    runtime_mode: str,
    launcher_pid: Optional[int] = None,
) -> None:
    pid_value = "" if launcher_pid is None else str(launcher_pid)
    paths.metadata_path.write_text(
        "\n".join(
            [
                f"run_id={paths.run_id}",
                f"variant={variant}",
                f"chunk_size={chunk_size}",
                f"container_dir={container_dir}",
                f"temp_mode={temp_mode}",
                f"config={paths.config_path}",
                f"log={paths.log_file}",
                f"status={paths.status_file}",
                f"launcher_pid={pid_value}",
                f"staged_root={paths.staged_root}",
                f"source_staged={source_staged}",
                f"container_runtime_mode={runtime_mode}",
                "",
            ]
        ),
        newline="\n",
    )


def generate_diagnostic_run(
    variant: str,
    chunk_size: int,
    max_chunks: int = DEFAULT_MAX_CHUNKS,
    environ: Optional[Mapping[str, str]] = None,
    now: Optional[datetime] = None,
    check_source: bool = True,
    create_staged_link: bool = True,
) -> DiagnosticPaths:
    """Generate config, launcher, metadata, and staged-source symlink."""

    env = os.environ if environ is None else environ
    project_root = _env("CPTOOLS2_PROJECT_ROOT", env)
    container_dir = _env("CPTOOLS2_CONTAINER_DIR", env)
    source_staged = _env("CPTOOLS2_DIAG_STAGED_SOURCE", env)
    diag_root = Path(env.get("CPTOOLS2_DIAG_ROOT") or _default_diag_root(env))
    plate_id = env.get("CPTOOLS2_DIAG_PLATE_ID") or DEFAULT_PLATE_ID
    temp_mode = env.get("CPTOOLS2_DIAG_TEMP_MODE") or "scratch"
    runtime_mode = _runtime_mode(env)
    stages_value = env.get("CPTOOLS2_DIAG_STAGES") or ",".join(DEFAULT_STAGES)
    stages = [
        value.strip()
        for value in stages_value.split(",")
        if value.strip()
    ]

    if check_source and not Path(source_staged).is_dir():
        raise FileNotFoundError(f"Missing staged source: {source_staged}")

    paths = _build_paths(variant, chunk_size, diag_root, now=now)
    paths.run_dir.mkdir(parents=True, exist_ok=True)
    paths.log_dir.mkdir(parents=True, exist_ok=True)
    paths.staged_root.mkdir(parents=True, exist_ok=True)

    if create_staged_link:
        staged_link = paths.staged_root / plate_id
        if staged_link.exists() or staged_link.is_symlink():
            staged_link.unlink()
        staged_link.symlink_to(source_staged, target_is_directory=True)

    config = build_config(
        paths,
        plate_id,
        chunk_size,
        container_dir,
        stages=stages,
        max_chunks=max_chunks,
    )
    paths.config_path.write_text(yaml.safe_dump(config, sort_keys=False), newline="\n")
    paths.launch_path.write_text(build_launcher(), newline="\n")
    paths.launch_path.chmod(0o755)

    _write_metadata(
        paths=paths,
        variant=variant,
        chunk_size=chunk_size,
        container_dir=container_dir,
        temp_mode=temp_mode,
        source_staged=source_staged,
        runtime_mode=runtime_mode,
    )
    # Validate project_root eagerly so missing env reports before optional launch.
    if not project_root:
        raise ValueError("CPTOOLS2_PROJECT_ROOT is required")
    return paths


def launch_diagnostic_run(
    paths: DiagnosticPaths,
    project_root: str,
    container_dir: str,
    temp_mode: str,
) -> int:
    """Start the launcher in the background and return its process id."""

    nohup_log = paths.log_dir / "nohup.log"
    with nohup_log.open("wb") as out:
        proc = subprocess.Popen(
            [
                "bash",
                str(paths.launch_path),
                project_root,
                str(paths.config_path),
                str(paths.status_file),
                str(paths.log_file),
                temp_mode,
                container_dir,
            ],
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=subprocess.STDOUT,
        )
    return proc.pid


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a scratch-only cptools2 Singularity/FUSE diagnostic run."
    )
    parser.add_argument("variant", help="diagnostic variant name")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=int(os.environ.get("CPTOOLS2_DIAG_CHUNK_SIZE", DEFAULT_CHUNK_SIZE)),
        help="configured cptools2 chunk size",
    )
    parser.add_argument(
        "--max-chunks",
        type=int,
        default=int(os.environ.get("CPTOOLS2_DIAG_MAX_CHUNKS", DEFAULT_MAX_CHUNKS)),
        help="maximum number of generated chunk files to execute",
    )
    parser.add_argument(
        "--launch",
        action="store_true",
        help="start the generated launcher in the background",
    )
    parser.add_argument(
        "--skip-source-check",
        action="store_true",
        help="generate files even if CPTOOLS2_DIAG_STAGED_SOURCE is not visible",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    paths = generate_diagnostic_run(
        variant=args.variant,
        chunk_size=args.chunk_size,
        max_chunks=args.max_chunks,
        check_source=not args.skip_source_check,
    )
    pid = None
    if args.launch:
        env = os.environ
        project_root = _env("CPTOOLS2_PROJECT_ROOT", env)
        container_dir = _env("CPTOOLS2_CONTAINER_DIR", env)
        temp_mode = env.get("CPTOOLS2_DIAG_TEMP_MODE") or "scratch"
        pid = launch_diagnostic_run(paths, project_root, container_dir, temp_mode)
        _write_metadata(
            paths=paths,
            variant=args.variant,
            chunk_size=args.chunk_size,
            container_dir=container_dir,
            temp_mode=temp_mode,
            source_staged=_env("CPTOOLS2_DIAG_STAGED_SOURCE", env),
            runtime_mode=_runtime_mode(env),
            launcher_pid=pid,
        )

    print(f"RUN_ID={paths.run_id}")
    print(f"RUN_DIR={paths.run_dir}")
    if pid is not None:
        print(f"PID={pid}")
    print(f"CONFIG={paths.config_path}")
    print(f"LOG={paths.log_file}")
    print(f"STATUS={paths.status_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
