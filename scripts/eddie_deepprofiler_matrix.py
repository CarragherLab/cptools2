"""Build Eddie DeepProfiler scalability matrix runs.

The runner writes route-specific YAML configs under the scratch diagnostics
tree and emits a JSON matrix that contains the remote command for each route.
"""

from __future__ import annotations

import argparse
import json
import shlex
from pathlib import Path
from typing import Optional

import yaml

DEFAULT_ROUTE_ROOT = "diagnostics/deepprofiler-scalability"
LOCK_DIR = "${CPTOOLS2_SCRATCH_ROOT}/locks/deepprofiler"


def _base_env() -> dict[str, str]:
    return {
        "CPTOOLS2_GPU_QUEUE": "gpu",
        "CPTOOLS2_GPU_RESOURCE": "-l gpu=1",
        "CPTOOLS2_FEATURE_GPU_QUEUE": "gpu",
        "CPTOOLS2_FEATURE_GPU_RESOURCE": "-l gpu=1",
        "CPTOOLS2_SEGMENT_GPU_QUEUE": "gpu",
        "CPTOOLS2_SEGMENT_GPU_RESOURCE": "-l gpu=1",
        "CPTOOLS2_SEGMENT_MAX_FORKS": "5",
        "CPTOOLS2_DEEPPROFILER_LOCK_DIR": LOCK_DIR,
    }


def build_matrix(
    max_chunks: int | None,
    chunk: int,
    route_root: str = DEFAULT_ROUTE_ROOT,
) -> list[dict[str, object]]:
    """Return the DeepProfiler scalability matrix definition."""

    routes = [
        ("dp-serialized", 1, "false", "false"),
        ("dp-concurrency2", 2, "false", "false"),
        ("dp-hostlock-concurrency4", 4, "false", "true"),
        ("dp-growth-concurrency2", 2, "true", "false"),
        ("dp-growth-concurrency5", 5, "true", "false"),
        ("dp-growth-concurrency8", 8, "true", "false"),
        ("dp-growth-hostlock-concurrency4", 4, "true", "true"),
    ]

    matrix: list[dict[str, object]] = []
    for name, max_forks, tf_allow_growth, host_lock in routes:
        env = _base_env()
        env.update(
            {
                "CPTOOLS2_FEATURE_MAX_FORKS": str(max_forks),
                "CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH": tf_allow_growth,
                "CPTOOLS2_DEEPPROFILER_HOST_LOCK": host_lock,
            }
        )
        entry: dict[str, object] = {
            "name": name,
            "chunk": chunk,
            "env": env,
            "route_root": route_root,
        }
        if max_chunks is not None:
            entry["max_chunks"] = max_chunks
        matrix.append(entry)
    return matrix


def _export_lines(env: dict[str, str]) -> list[str]:
    return [f"export {key}={shlex.quote(value)}" for key, value in env.items()]


def write_route_config(
    entry: dict[str, object],
    *,
    base_config: Path,
    route_dir: Path,
) -> Path:
    """Write a route-specific YAML config and return the written path."""

    config = yaml.safe_load(base_config.read_text()) or {}
    route_dir.mkdir(parents=True, exist_ok=True)
    config["chunk"] = int(entry["chunk"])
    if entry.get("max_chunks") is None:
        config.pop("max_chunks", None)
    else:
        config["max_chunks"] = int(entry["max_chunks"])
    config["output_dir"] = f"{route_dir.as_posix()}/outputs"
    if "commands location" in config:
        config["commands location"] = f"{route_dir.as_posix()}/commands"

    route_config = route_dir / "route_config.yml"
    route_config.write_text(yaml.safe_dump(config, sort_keys=False))
    return route_config


def render_remote_command(
    entry: dict[str, object],
    *,
    project_root: str,
    scratch_root: str,
    config_path: str,
) -> str:
    """Render the remote Eddie command for a single route."""

    name = str(entry["name"])
    route_root = str(entry.get("route_root", DEFAULT_ROUTE_ROOT))
    run_root = f"{scratch_root}/{route_root}/{name}"
    route_config = f"{run_root}/route_config.yml"
    env = dict(entry["env"])

    command_lines = [
        "set -euo pipefail",
        f"cd {shlex.quote(project_root)}",
        f"export CPTOOLS2_PROJECT_ROOT={shlex.quote(project_root)}",
        f"export CPTOOLS2_SCRATCH_ROOT={shlex.quote(scratch_root)}",
        "source config/eddie_env.sh",
        *_export_lines(env),
        f"mkdir -p {shlex.quote(run_root)}",
        "python3 -m cptools2.__main__ pipeline " + shlex.quote(route_config),
    ]
    return "\n".join(command_lines)


def _build_route_entry(
    entry: dict[str, object],
    *,
    project_root: str,
    scratch_root: str,
    base_config: Path,
) -> dict[str, object]:
    route_root = str(entry.get("route_root", DEFAULT_ROUTE_ROOT))
    run_root = Path(scratch_root) / route_root / str(entry["name"])
    route_config = write_route_config(
        entry,
        base_config=base_config,
        route_dir=run_root,
    )
    return {
        **entry,
        "run_root": str(run_root),
        "route_config": str(route_config),
        "output_root": str(run_root / "outputs"),
        "remote_command": render_remote_command(
            entry,
            project_root=project_root,
            scratch_root=scratch_root,
            config_path=str(base_config),
        ),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate DeepProfiler scalability routes for Eddie."
    )
    parser.add_argument("--chunk", type=int, required=True)
    parser.add_argument("--max-chunks", type=int)
    parser.add_argument("--route-root", default=DEFAULT_ROUTE_ROOT)
    parser.add_argument("--project-root", required=True)
    parser.add_argument("--scratch-root", required=True)
    parser.add_argument("--config-path", required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    base_config = Path(args.config_path)
    matrix = build_matrix(
        max_chunks=args.max_chunks,
        chunk=args.chunk,
        route_root=args.route_root,
    )
    rendered = [
        _build_route_entry(
            entry,
            project_root=args.project_root,
            scratch_root=args.scratch_root,
            base_config=base_config,
        )
        for entry in matrix
    ]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(rendered, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
