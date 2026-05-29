"""Join/staging helpers for plate-level feature export."""

from __future__ import annotations

import csv
import shutil
from pathlib import Path
from typing import Sequence, Tuple


def stage_feature_payloads(
    *,
    plate_id: object,
    chunk_ids: Sequence[object],
    feature_dirs: Sequence[object],
    export_input_dir: Path,
) -> Tuple[Path, ...]:
    """Stage chunk payloads into unique chunk-scoped inputs.

    Each payload is copied under ``export_input_dir/chunk_<chunk_id>/features``.
    Duplicate chunk ids are rejected before any filesystem merge occurs.
    """

    plate_id = "" if plate_id is None else str(plate_id).strip()
    if not plate_id:
        raise ValueError("plate_id must not be empty")

    chunk_ids = [
        "" if chunk_id is None else str(chunk_id).strip() for chunk_id in chunk_ids
    ]
    feature_dirs = [Path(feature_dir) for feature_dir in feature_dirs]

    if len(chunk_ids) != len(feature_dirs):
        raise ValueError(
            "Mismatched chunk_ids and feature_dirs counts: "
            f"{len(chunk_ids)} != {len(feature_dirs)}"
        )

    seen = set()
    duplicates = set()
    for chunk_id in chunk_ids:
        if chunk_id in seen:
            duplicates.add(chunk_id)
        else:
            seen.add(chunk_id)
    duplicates = sorted(duplicates)
    if duplicates:
        raise ValueError(
            "Duplicate chunk_ids are not allowed: " + ", ".join(duplicates)
        )

    export_input_dir = Path(export_input_dir)
    staged_paths = []
    manifest_rows = []
    for chunk_id, feature_dir in zip(chunk_ids, feature_dirs):
        if not feature_dir.exists():
            raise FileNotFoundError(f"Missing feature payload directory: {feature_dir}")
        target = export_input_dir / f"chunk_{chunk_id}" / "features"
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        shutil.copytree(feature_dir, target, dirs_exist_ok=False)
        staged_paths.append(target)
        manifest_rows.append(
            {
                "plate_id": plate_id,
                "chunk_id": chunk_id,
                "source_feature_dir": str(feature_dir),
                "staged_feature_dir": str(target),
            }
        )

    export_input_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = export_input_dir / "export_input_manifest.csv"
    with manifest_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "plate_id",
                "chunk_id",
                "source_feature_dir",
                "staged_feature_dir",
            ],
        )
        writer.writeheader()
        writer.writerows(manifest_rows)
    return tuple(staged_paths)
