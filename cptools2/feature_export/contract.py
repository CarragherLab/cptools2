"""Shared contracts for feature export."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

DEFAULT_FORMATS = ("csv",)
SUPPORTED_FORMATS = ("csv", "parquet")
DEFAULT_NAN_OBJECT_FAIL_FRACTION = 0.05

MANIFEST_TABLE = "deepprofiler_manifest"
SITE_TABLE = "deepprofiler_sites"
CELL_TABLE = "deepprofiler_cells"
QUALITY_TABLE = "deepprofiler_quality"

REQUIRED_COMMON_METADATA = (
    "Metadata_Plate",
    "Metadata_Well",
    "Metadata_Site",
    "Metadata_ImageSet",
    "Metadata_Object",
    "Metadata_SourceFile",
    "Metadata_FeatureExtractor",
)


def normalise_formats(values: Optional[Iterable[str]]) -> Tuple[str, ...]:
    """Return supported output formats in a stable order."""

    if values is None:
        return DEFAULT_FORMATS

    requested = set()
    for value in values:
        text = str(value).strip().lower()
        if text:
            requested.add(text)

    unsupported = sorted(requested.difference(SUPPORTED_FORMATS))
    if unsupported:
        raise ValueError("Unsupported feature export format: " + ", ".join(unsupported))

    ordered = tuple(value for value in SUPPORTED_FORMATS if value in requested)
    return ordered or DEFAULT_FORMATS


def published_table_name(
    plate_id: str,
    table_stem: str,
    run_label: Optional[str] = None,
    suffix: str = ".csv",
) -> str:
    """Return a plate-aware published filename for a canonical table stem."""

    parts = [str(plate_id).strip()]
    if run_label:
        parts.append(str(run_label).strip())
    parts.append(table_stem)
    return "__".join(parts) + suffix


@dataclass(frozen=True)
class FeatureExportRequest:
    extractor: str
    features_dir: Path
    output_dir: Path
    formats: Tuple[str, ...] = DEFAULT_FORMATS
    nan_object_fail_fraction: float = DEFAULT_NAN_OBJECT_FAIL_FRACTION
    plate_id: Optional[str] = None
    run_label: Optional[str] = None

    def __post_init__(self) -> None:
        if not 0 <= self.nan_object_fail_fraction <= 1:
            raise ValueError(
                "nan_object_fail_fraction must be between 0 and 1, got {}".format(
                    self.nan_object_fail_fraction
                )
            )

    @property
    def tables_dir(self) -> Path:
        return self.output_dir / "tables"


@dataclass(frozen=True)
class FeatureExportResult:
    extractor: str
    manifest_rows: int
    site_rows: int
    cell_rows: int
    quality_rows: int
    written_files: Tuple[Path, ...]
