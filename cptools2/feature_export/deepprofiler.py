"""DeepProfiler native-output exporter."""

from __future__ import annotations

import csv
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from .contract import (
    CELL_TABLE,
    MANIFEST_TABLE,
    QUALITY_TABLE,
    SITE_TABLE,
    FeatureExportRequest,
    FeatureExportResult,
    published_table_name,
)
from .writers import write_table

FEATURE_KEY_CANDIDATES = ("features", "embeddings", "arr_0")


@dataclass(frozen=True)
class DeepProfilerNpzSchema:
    path: Path
    kind: str
    feature_key: str
    n_rows: int
    n_features: int
    value_count: int
    nan_count: int
    nan_object_count: int
    has_nan: bool
    all_nan: bool
    dtype: str
    shape: str
    keys: str
    reason: str = ""


CELL_COLUMNS = [
    "Metadata_Plate",
    "Metadata_Well",
    "Metadata_Site",
    "Metadata_ImageSet",
    "Metadata_Object",
    "Metadata_SourceFile",
    "Metadata_FeatureExtractor",
    "Metadata_Center_X",
    "Metadata_Center_Y",
    "Metadata_MaskPath",
    "Metadata_Object_NaN_Count",
    "Metadata_Object_NaN_Fraction",
    "Metadata_Object_All_NaN",
    "Metadata_Object_Has_NaN",
    "Metadata_Object_QualityStatus",
]

SITE_COLUMNS = [
    "Metadata_Plate",
    "Metadata_Well",
    "Metadata_Site",
    "Metadata_ImageSet",
    "Metadata_Object_Count",
    "Metadata_SourceFile",
    "Metadata_FeatureExtractor",
    "Metadata_NaN_Count",
    "Metadata_NaN_Fraction",
    "Metadata_NaN_Object_Count",
    "Metadata_NaN_Object_Fraction",
    "Metadata_All_NaN",
    "Metadata_QualityGate",
]

MANIFEST_COLUMNS = [
    "Metadata_Plate",
    "Metadata_Well",
    "Metadata_Site",
    "Metadata_ImageSet",
    "Metadata_SourceFile",
    "Metadata_FeatureExtractor",
    "Metadata_NpzKeys",
    "Metadata_FeatureKey",
    "Metadata_FeatureShape",
    "Metadata_FeatureDType",
    "Metadata_Object_Count",
    "Metadata_Feature_Count",
    "Metadata_NaN_Object_Count",
    "Metadata_NaN_Object_Fraction",
    "Metadata_NaN_Object_Fail_Fraction",
    "Metadata_ExportStatus",
    "Metadata_Reason",
]

QUALITY_COLUMNS = [
    "Metadata_Plate",
    "Metadata_Well",
    "Metadata_Site",
    "Metadata_ImageSet",
    "Metadata_SourceFile",
    "Metadata_Object_Count",
    "Metadata_Feature_Count",
    "Metadata_Value_Count",
    "Metadata_NaN_Count",
    "Metadata_NaN_Fraction",
    "Metadata_NaN_Object_Count",
    "Metadata_NaN_Object_Fraction",
    "Metadata_NaN_Object_Fail_Fraction",
    "Metadata_All_NaN",
    "Metadata_Has_NaN",
    "Metadata_QualityStatus",
    "Metadata_QualityGate",
]


def inspect_npz(path: Path) -> DeepProfilerNpzSchema:
    """Inspect one DeepProfiler npz feature file."""

    path = Path(path)
    with np.load(path, allow_pickle=True) as data:
        keys = ",".join(data.files)
        feature_key = _select_feature_key(data.files)
        if not feature_key:
            return _empty_schema(
                path=path,
                kind="missing_features",
                keys=keys,
                reason="No supported feature array key found",
            )
        array = data[feature_key]

    if array.ndim == 1:
        kind = "site"
        n_rows = 1
        n_features = int(array.shape[0])
    elif array.ndim == 2:
        kind = "cell"
        n_rows = int(array.shape[0])
        n_features = int(array.shape[1])
    else:
        return _empty_schema(
            path=path,
            kind="unsupported_schema",
            keys=keys,
            feature_key=feature_key,
            dtype=str(array.dtype),
            shape=str(tuple(array.shape)),
            reason="Expected 1D or 2D feature array, observed shape {}".format(
                tuple(array.shape)
            ),
        )

    if np.issubdtype(array.dtype, np.number):
        nan_count = int(np.isnan(array).sum())
        if array.ndim == 2:
            nan_object_count = int(np.isnan(array).any(axis=1).sum())
        else:
            nan_object_count = int(nan_count > 0)
    else:
        nan_count = 0
        nan_object_count = 0
    value_count = int(array.size)
    return DeepProfilerNpzSchema(
        path=path,
        kind=kind,
        feature_key=feature_key,
        n_rows=n_rows,
        n_features=n_features,
        value_count=value_count,
        nan_count=nan_count,
        nan_object_count=nan_object_count,
        has_nan=nan_count > 0,
        all_nan=value_count > 0 and nan_count == value_count,
        dtype=str(array.dtype),
        shape=str(tuple(array.shape)),
        keys=keys,
    )


def export_deepprofiler(request: FeatureExportRequest) -> FeatureExportResult:
    """Export DeepProfiler npz outputs to plate-level tables."""

    inspected: List[Tuple[Path, DeepProfilerNpzSchema, Dict[str, object]]] = []
    manifest_rows: List[Dict[str, object]] = []
    site_rows: List[Dict[str, object]] = []
    quality_rows: List[Dict[str, object]] = []
    cell_row_count = 0
    max_feature_count = 0

    for path in sorted(Path(request.features_dir).rglob("*.npz")):
        schema = inspect_npz(path)
        metadata = _metadata_from_npz_path(path, request.features_dir)
        inspected.append((path, schema, metadata))
        manifest_rows.append(_manifest_row(metadata, schema, request))
        quality_rows.append(_quality_row(metadata, schema, request))

        if schema.kind not in {"site", "cell"}:
            continue

        site_rows.append(_site_row(metadata, schema, request))
        if schema.kind == "cell":
            cell_row_count += schema.n_rows
            max_feature_count = max(max_feature_count, schema.n_features)

    plate_id = _resolve_plate_id(inspected, request.plate_id)
    feature_columns = _feature_columns(max_feature_count)
    cell_columns = CELL_COLUMNS + feature_columns
    written: List[Path] = []
    written.extend(
        write_table(
            manifest_rows,
            request.tables_dir / MANIFEST_TABLE,
            request.formats,
            columns=MANIFEST_COLUMNS,
        )
    )
    written.extend(
        write_table(
            site_rows,
            request.tables_dir / SITE_TABLE,
            request.formats,
            columns=SITE_COLUMNS,
        )
    )
    written.extend(
        _write_cell_tables(
            inspected=inspected,
            output_stem=request.tables_dir / CELL_TABLE,
            formats=request.formats,
            columns=cell_columns,
        )
    )
    written.extend(
        write_table(
            quality_rows,
            request.tables_dir / QUALITY_TABLE,
            request.formats,
            columns=QUALITY_COLUMNS,
        )
    )
    if plate_id:
        written = list(
            _publish_aliases(
                written,
                plate_id=plate_id,
                run_label=request.run_label,
                tables_dir=request.tables_dir,
            )
        )
    return FeatureExportResult(
        extractor=request.extractor,
        manifest_rows=len(manifest_rows),
        site_rows=len(site_rows),
        cell_rows=cell_row_count,
        quality_rows=len(quality_rows),
        written_files=tuple(written),
    )


def _metadata_from_npz_path(path: Path, features_dir: Path) -> Dict[str, object]:
    """Infer plate, well, and site from a native DeepProfiler payload path."""

    path = Path(path)
    features_dir = Path(features_dir)
    try:
        relative_path = path.relative_to(features_dir)
    except ValueError as exc:
        raise ValueError(
            "Expected DeepProfiler payload path features/<plate>/<well>/<site>.npz, "
            "observed: {}".format(path)
        ) from exc

    parts = _payload_suffix_parts(relative_path.parts)
    if parts is None:
        raise ValueError(
            "Expected DeepProfiler payload path features/<plate>/<well>/<site>.npz, "
            "observed: {}".format(path)
        )

    plate, well, site_file = parts
    site = Path(site_file).stem
    if not plate or not well or not site or Path(site_file).suffix != ".npz":
        raise ValueError(
            "Expected DeepProfiler payload path features/<plate>/<well>/<site>.npz, "
            "observed: {}".format(path)
        )

    return {
        "Metadata_Plate": plate,
        "Metadata_Well": well,
        "Metadata_Site": site,
        "Metadata_ImageSet": "{}_{}_s{}".format(plate, well, site),
        "Metadata_SourceFile": path.as_posix(),
        "Metadata_FeatureExtractor": "deepprofiler",
    }


def _payload_suffix_parts(parts: Sequence[str]) -> Optional[Tuple[str, str, str]]:
    if len(parts) == 3:
        plate, well, site_file = parts
        if Path(site_file).suffix == ".npz":
            return plate, well, site_file
    if (
        len(parts) == 5
        and parts[0].startswith("chunk_")
        and parts[1] == "features"
    ):
        plate, well, site_file = parts[2:]
        if Path(site_file).suffix == ".npz":
            return plate, well, site_file
    return None


def _resolve_plate_id(
    inspected: Sequence[Tuple[Path, DeepProfilerNpzSchema, Dict[str, object]]],
    requested_plate_id: Optional[str],
) -> str:
    inferred = sorted(
        {
            str(metadata["Metadata_Plate"])
            for _, schema, metadata in inspected
            if schema.kind in {"site", "cell", "missing_features", "unsupported_schema"}
            and metadata.get("Metadata_Plate")
        }
    )
    if len(inferred) > 1:
        raise ValueError(
            "Expected one plate in DeepProfiler payloads, found: {}".format(
                ", ".join(inferred)
            )
        )
    if requested_plate_id:
        requested = str(requested_plate_id)
        if inferred and inferred != [requested]:
            raise ValueError(
                "Requested mismatch: plate_id {} does not match observed plate(s): "
                "{}".format(requested, ", ".join(inferred))
            )
        return requested
    if len(inferred) == 1:
        return inferred[0]
    return ""


def _quality_status(schema: DeepProfilerNpzSchema) -> str:
    if schema.kind in {"missing_features", "unsupported_schema"}:
        return schema.kind
    if schema.all_nan:
        return "all_nan"
    if schema.has_nan:
        return "has_nan"
    return "pass"


def _quality_gate(schema: DeepProfilerNpzSchema, request: FeatureExportRequest) -> str:
    if schema.kind in {"missing_features", "unsupported_schema"}:
        return "fail_site"
    if schema.n_rows == 0 or schema.nan_object_count == 0:
        return "pass"
    if schema.nan_object_count == schema.n_rows:
        return "fail_site"
    if _nan_object_fraction(schema) > request.nan_object_fail_fraction:
        return "fail_site"
    return "warn"


def _bool_text(value: bool) -> str:
    return "true" if value else "false"


def _nan_fraction(schema: DeepProfilerNpzSchema) -> float:
    if schema.value_count == 0:
        return 0.0
    return schema.nan_count / schema.value_count


def _nan_object_fraction(schema: DeepProfilerNpzSchema) -> float:
    if schema.n_rows == 0:
        return 0.0
    return schema.nan_object_count / schema.n_rows


def _vector_nan_count(vector: np.ndarray) -> int:
    values = np.asarray(vector).reshape(-1)
    if np.issubdtype(values.dtype, np.number):
        return int(np.isnan(values).sum())
    return 0


def _manifest_row(
    metadata: Dict[str, object],
    schema: DeepProfilerNpzSchema,
    request: FeatureExportRequest,
) -> Dict[str, object]:
    return {
        "Metadata_Plate": metadata["Metadata_Plate"],
        "Metadata_Well": metadata["Metadata_Well"],
        "Metadata_Site": metadata["Metadata_Site"],
        "Metadata_ImageSet": metadata["Metadata_ImageSet"],
        "Metadata_SourceFile": metadata["Metadata_SourceFile"],
        "Metadata_FeatureExtractor": metadata["Metadata_FeatureExtractor"],
        "Metadata_NpzKeys": schema.keys,
        "Metadata_FeatureKey": schema.feature_key,
        "Metadata_FeatureShape": schema.shape,
        "Metadata_FeatureDType": schema.dtype,
        "Metadata_Object_Count": schema.n_rows,
        "Metadata_Feature_Count": schema.n_features,
        "Metadata_NaN_Object_Count": schema.nan_object_count,
        "Metadata_NaN_Object_Fraction": _nan_object_fraction(schema),
        "Metadata_NaN_Object_Fail_Fraction": request.nan_object_fail_fraction,
        "Metadata_ExportStatus": _quality_status(schema),
        "Metadata_Reason": schema.reason,
    }


def _site_row(
    metadata: Dict[str, object],
    schema: DeepProfilerNpzSchema,
    request: FeatureExportRequest,
) -> Dict[str, object]:
    return {
        "Metadata_Plate": metadata["Metadata_Plate"],
        "Metadata_Well": metadata["Metadata_Well"],
        "Metadata_Site": metadata["Metadata_Site"],
        "Metadata_ImageSet": metadata["Metadata_ImageSet"],
        "Metadata_Object_Count": schema.n_rows,
        "Metadata_SourceFile": metadata["Metadata_SourceFile"],
        "Metadata_FeatureExtractor": metadata["Metadata_FeatureExtractor"],
        "Metadata_NaN_Count": schema.nan_count,
        "Metadata_NaN_Fraction": _nan_fraction(schema),
        "Metadata_NaN_Object_Count": schema.nan_object_count,
        "Metadata_NaN_Object_Fraction": _nan_object_fraction(schema),
        "Metadata_All_NaN": _bool_text(schema.all_nan),
        "Metadata_QualityGate": _quality_gate(schema, request),
    }


def _quality_row(
    metadata: Dict[str, object],
    schema: DeepProfilerNpzSchema,
    request: FeatureExportRequest,
) -> Dict[str, object]:
    return {
        "Metadata_Plate": metadata["Metadata_Plate"],
        "Metadata_Well": metadata["Metadata_Well"],
        "Metadata_Site": metadata["Metadata_Site"],
        "Metadata_ImageSet": metadata["Metadata_ImageSet"],
        "Metadata_SourceFile": metadata["Metadata_SourceFile"],
        "Metadata_Object_Count": schema.n_rows,
        "Metadata_Feature_Count": schema.n_features,
        "Metadata_Value_Count": schema.value_count,
        "Metadata_NaN_Count": schema.nan_count,
        "Metadata_NaN_Fraction": _nan_fraction(schema),
        "Metadata_NaN_Object_Count": schema.nan_object_count,
        "Metadata_NaN_Object_Fraction": _nan_object_fraction(schema),
        "Metadata_NaN_Object_Fail_Fraction": request.nan_object_fail_fraction,
        "Metadata_All_NaN": _bool_text(schema.all_nan),
        "Metadata_Has_NaN": _bool_text(schema.has_nan),
        "Metadata_QualityStatus": _quality_status(schema),
        "Metadata_QualityGate": _quality_gate(schema, request),
    }


def _cell_rows(
    metadata: Dict[str, object], features: np.ndarray, locations: Optional[np.ndarray]
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for index, vector in enumerate(features):
        row: Dict[str, object] = {
            "Metadata_Plate": metadata["Metadata_Plate"],
            "Metadata_Well": metadata["Metadata_Well"],
            "Metadata_Site": metadata["Metadata_Site"],
            "Metadata_ImageSet": metadata["Metadata_ImageSet"],
            "Metadata_Object": str(index + 1),
            "Metadata_SourceFile": metadata["Metadata_SourceFile"],
            "Metadata_FeatureExtractor": metadata["Metadata_FeatureExtractor"],
            "Metadata_Center_X": None,
            "Metadata_Center_Y": None,
            "Metadata_MaskPath": None,
        }
        nan_count = _vector_nan_count(vector)
        feature_count = int(np.asarray(vector).reshape(-1).size)
        row["Metadata_Object_NaN_Count"] = nan_count
        row["Metadata_Object_NaN_Fraction"] = (
            nan_count / feature_count if feature_count else 0.0
        )
        row["Metadata_Object_All_NaN"] = _bool_text(
            feature_count > 0 and nan_count == feature_count
        )
        row["Metadata_Object_Has_NaN"] = _bool_text(nan_count > 0)
        row["Metadata_Object_QualityStatus"] = (
            "all_nan"
            if feature_count > 0 and nan_count == feature_count
            else "has_nan" if nan_count > 0 else "pass"
        )
        center = _center_from_locations(locations, index)
        if center is not None:
            row["Metadata_Center_X"] = center[0]
            row["Metadata_Center_Y"] = center[1]
        row.update(_feature_values(vector))
        rows.append(row)
    return rows


def _center_from_locations(
    locations: Optional[np.ndarray], index: int
) -> Optional[Sequence[float]]:
    if locations is None:
        return None
    if index >= len(locations):
        return None
    location = np.asarray(locations[index]).reshape(-1)
    if location.size < 2:
        return None
    try:
        return float(location[0]), float(location[1])
    except (TypeError, ValueError):
        return None


def _feature_values(vector: np.ndarray) -> Dict[str, object]:
    values = np.asarray(vector).reshape(-1)
    return {
        "DeepProfiler_Feature_{:04d}".format(index): float(value)
        for index, value in enumerate(values)
    }


def _feature_columns(count: int) -> List[str]:
    return ["DeepProfiler_Feature_{:04d}".format(index) for index in range(count)]


def _write_cell_tables(
    *,
    inspected: Sequence[Tuple[Path, DeepProfilerNpzSchema, Dict[str, object]]],
    output_stem: Path,
    formats: Sequence[str],
    columns: Sequence[str],
) -> Tuple[Path, ...]:
    written: List[Path] = []
    if "csv" in formats:
        written.append(
            _write_cell_csv(inspected, output_stem.with_suffix(".csv"), columns)
        )
    parquet_formats = tuple(fmt for fmt in formats if fmt == "parquet")
    if parquet_formats:
        rows: List[Dict[str, object]] = []
        for path, schema, metadata in inspected:
            if schema.kind != "cell":
                continue
            with np.load(path, allow_pickle=True) as data:
                features = data[schema.feature_key]
                locations = data["locations"] if "locations" in data.files else None
            rows.extend(_cell_rows(metadata, features, locations))
        written.extend(
            write_table(
                rows,
                output_stem,
                parquet_formats,
                columns=columns,
            )
        )
    return tuple(written)


def _write_cell_csv(
    inspected: Sequence[Tuple[Path, DeepProfilerNpzSchema, Dict[str, object]]],
    output_path: Path,
    columns: Sequence[str],
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        for path, schema, metadata in inspected:
            if schema.kind != "cell":
                continue
            with np.load(path, allow_pickle=True) as data:
                features = data[schema.feature_key]
                locations = data["locations"] if "locations" in data.files else None
            for row in _cell_rows(metadata, features, locations):
                writer.writerow(row)
    return output_path


def _publish_aliases(
    written_files: Sequence[Path],
    *,
    plate_id: str,
    run_label: Optional[str],
    tables_dir: Path,
) -> Tuple[Path, ...]:
    published: List[Path] = list(written_files)
    for path in written_files:
        if path.parent != tables_dir:
            continue
        stem = path.stem
        if not stem.startswith("deepprofiler_"):
            continue
        alias = tables_dir / published_table_name(plate_id, stem, suffix=path.suffix)
        shutil.copy2(path, alias)
        published.append(alias)
        if run_label:
            run_alias = tables_dir / published_table_name(
                plate_id,
                stem,
                run_label=run_label,
                suffix=path.suffix,
            )
            shutil.copy2(path, run_alias)
            published.append(run_alias)
    return tuple(published)


def _empty_schema(
    *,
    path: Path,
    kind: str,
    keys: str,
    reason: str,
    feature_key: str = "",
    dtype: str = "",
    shape: str = "",
) -> DeepProfilerNpzSchema:
    return DeepProfilerNpzSchema(
        path=path,
        kind=kind,
        feature_key=feature_key,
        n_rows=0,
        n_features=0,
        value_count=0,
        nan_count=0,
        nan_object_count=0,
        has_nan=False,
        all_nan=False,
        dtype=dtype,
        shape=shape,
        keys=keys,
        reason=reason,
    )


def _select_feature_key(keys: Sequence[str]) -> str:
    for candidate in FEATURE_KEY_CANDIDATES:
        if candidate in keys:
            return candidate
    return ""
