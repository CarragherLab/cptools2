"""Feature export status and summary reports."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

PLATE_EXPORT_STATUS_COLUMNS = (
    "plate_id",
    "export_status",
    "failure_stage",
    "failure_reason",
    "tables_present",
    "manifest_path",
)

PLATE_EXPORT_STATUS_FILENAME = "feature_export_status.csv"
PLATE_EXPORT_README_FILENAME = "README.md"
SUMMARY_EXPORT_FILENAME = "feature_export_summary.csv"
SUMMARY_README_FILENAME = "README.md"

FAILURE_STAGE_PATTERNS = (
    (
        "staging_validation",
        (
            "duplicate chunk_ids",
            "mismatched chunk_ids",
            "missing feature payload directory",
            "plate_id must not be empty",
        ),
    ),
    (
        "payload_validation",
        (
            "expected deepprofiler payload path",
            "no supported feature array key found",
            "expected 1d or 2d feature array",
        ),
    ),
    (
        "plate_identity_validation",
        (
            "expected one plate in deepprofiler payloads",
            "requested mismatch",
            "does not match exported plate(s)",
        ),
    ),
)


@dataclass(frozen=True)
class PlateExportStatus:
    plate_id: str
    export_status: str
    failure_stage: str
    failure_reason: str
    tables_present: bool
    manifest_path: str

    def as_row(self) -> Dict[str, str]:
        return {
            "plate_id": self.plate_id,
            "export_status": self.export_status,
            "failure_stage": self.failure_stage,
            "failure_reason": self.failure_reason,
            "tables_present": _bool_text(self.tables_present),
            "manifest_path": self.manifest_path,
        }


def write_plate_export_report(
    *,
    output_dir: Path,
    plate_id: object,
    export_status: str,
    failure_stage: str,
    failure_reason: str,
    tables_present: bool,
    manifest_path: object,
) -> Tuple[Path, Path]:
    """Write a machine-readable per-plate status row and README."""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    status = PlateExportStatus(
        plate_id=_clean_text(plate_id),
        export_status=_clean_text(export_status),
        failure_stage=_clean_text(failure_stage),
        failure_reason=_clean_text(failure_reason),
        tables_present=bool(tables_present),
        manifest_path=_clean_path_text(manifest_path),
    )

    status_path = output_dir / PLATE_EXPORT_STATUS_FILENAME
    readme_path = output_dir / PLATE_EXPORT_README_FILENAME

    _write_status_csv(status_path, status)
    readme_path.write_text(_render_plate_readme(status), encoding="utf-8")
    return status_path, readme_path


def read_plate_export_status(status_path: Path) -> PlateExportStatus:
    """Read one per-plate status row from a status CSV file."""

    status_path = _status_csv_path(status_path)
    with status_path.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    if len(rows) != 1:
        raise ValueError(
            "Expected one plate export status row in {}".format(status_path)
        )

    row = rows[0]
    missing_columns = [
        column for column in PLATE_EXPORT_STATUS_COLUMNS if column not in row
    ]
    if missing_columns:
        raise ValueError(
            "Missing plate export status column(s) in {}: {}".format(
                status_path, ", ".join(missing_columns)
            )
        )

    return PlateExportStatus(
        plate_id=_clean_text(row["plate_id"]),
        export_status=_clean_text(row["export_status"]),
        failure_stage=_clean_text(row["failure_stage"]),
        failure_reason=_clean_text(row["failure_reason"]),
        tables_present=_is_truthy(row["tables_present"]),
        manifest_path=_clean_path_text(row["manifest_path"]),
    )


def write_feature_export_summary(
    *,
    output_dir: Path,
    expected_plate_ids: Sequence[object],
    observed_status_paths: Sequence[object],
) -> Tuple[Path, Path]:
    """Write a run-level feature export summary for expected plate ids."""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    expected = [_clean_text(plate_id) for plate_id in expected_plate_ids]
    observed: Dict[str, PlateExportStatus] = {}
    unexpected_observed: List[str] = []

    for status_path in observed_status_paths:
        status = read_plate_export_status(Path(status_path))
        if status.plate_id in observed:
            raise ValueError(
                "Duplicate plate export status observed for {}".format(
                    status.plate_id
                )
            )
        observed[status.plate_id] = status
        if status.plate_id not in expected:
            unexpected_observed.append(status.plate_id)

    rows: List[PlateExportStatus] = []
    for plate_id in expected:
        status = observed.get(plate_id)
        if status is None:
            rows.append(
                PlateExportStatus(
                    plate_id=plate_id,
                    export_status="missing_status",
                    failure_stage="missing_status",
                    failure_reason=(
                        "No export status artifact was produced for plate {}".format(
                            plate_id
                        )
                    ),
                    tables_present=False,
                    manifest_path="",
                )
            )
        else:
            rows.append(status)

    summary_path = output_dir / SUMMARY_EXPORT_FILENAME
    readme_path = output_dir / SUMMARY_README_FILENAME
    _write_status_csv(summary_path, rows)
    readme_path.write_text(
        _render_summary_readme(
            rows=rows,
            unexpected_observed=unexpected_observed,
        ),
        encoding="utf-8",
    )
    return summary_path, readme_path


def classify_plate_export_failure(exc: BaseException) -> Optional[str]:
    """Map a strict export exception to the stage that failed.

    Returns ``None`` for unexpected exceptions that should continue to fail the
    task without being converted into a plate-local status row.
    """

    lowered = str(exc).lower()
    for stage, patterns in FAILURE_STAGE_PATTERNS:
        if any(pattern in lowered for pattern in patterns):
            return stage
    return None


def _write_status_csv(path: Path, rows: Sequence[PlateExportStatus]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(rows, PlateExportStatus):
        rows = [rows]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(PLATE_EXPORT_STATUS_COLUMNS),
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_row())


def _render_plate_readme(status: PlateExportStatus) -> str:
    lines = [
        f"# Feature export report for plate {status.plate_id}",
        "",
        f"Export status: {status.export_status}",
        "",
        "Validated tables live under `tables/` in this plate output directory.",
        "",
        "- Status artifact: `{}".format(PLATE_EXPORT_STATUS_FILENAME),
        "- Export input manifest: `{}`".format(
            status.manifest_path or "unavailable"
        ),
    ]

    if status.export_status == "exported" and status.tables_present:
        lines.insert(
            5,
            "Validated export tables are available and were published successfully.",
        )
    else:
        lines.insert(5, "No measurement tables were published for this plate.")
        if status.failure_stage:
            lines.extend(
                [
                    "",
                    f"Failure stage: {status.failure_stage}",
                ]
            )
        if status.failure_reason:
            lines.extend(
                [
                    "",
                    f"Failure reason: {status.failure_reason}",
                ]
            )

    return "\n".join(lines) + "\n"


def _render_summary_readme(
    *,
    rows: Sequence[PlateExportStatus],
    unexpected_observed: Sequence[str],
) -> str:
    exported_rows = [row for row in rows if row.export_status == "exported"]
    failed_rows = [row for row in rows if row.export_status == "failed"]
    missing_rows = [row for row in rows if row.export_status == "missing_status"]
    other_rows = [
        row
        for row in rows
        if row.export_status not in {"exported", "failed", "missing_status"}
    ]

    lines = [
        "# Feature export summary",
        "",
        "CSV: `{}".format(SUMMARY_EXPORT_FILENAME),
        "",
        f"Expected plates: {len(rows)}",
        f"Successful plates: {len(exported_rows)}",
        f"Failed plates: {len(failed_rows)}",
        f"Missing status plates: {len(missing_rows)}",
    ]

    if failed_rows:
        lines.extend(["", "## Failed plates"])
        for row in failed_rows:
            reason = row.failure_reason or "No failure reason was recorded."
            lines.append(f"- `{row.plate_id}`: {reason}")

    if missing_rows:
        lines.extend(["", "## Missing status plates"])
        for row in missing_rows:
            lines.append(f"- `{row.plate_id}`: {row.failure_reason}")

    if unexpected_observed:
        lines.extend(["", "## Unexpected observed statuses"])
        for plate_id in sorted(set(unexpected_observed)):
            lines.append(f"- `{plate_id}`")

    if other_rows:
        lines.extend(["", "## Other statuses"])
        for row in other_rows:
            lines.append(f"- `{row.plate_id}`: {row.export_status}")

    return "\n".join(lines) + "\n"


def _status_csv_path(status_path: Path) -> Path:
    status_path = Path(status_path)
    if status_path.is_dir():
        status_path = status_path / PLATE_EXPORT_STATUS_FILENAME
    return status_path


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _clean_path_text(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    return Path(text).as_posix()


def _bool_text(value: bool) -> str:
    return "true" if value else "false"


def _is_truthy(value: object) -> bool:
    text = _clean_text(value).lower()
    return text in {"1", "true", "yes", "y", "on"}
