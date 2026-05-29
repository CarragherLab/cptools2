# Feature Export Tables And Measurement Contracts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a feature-export phase that converts method-native feature outputs into export-ready CSV measurement tables with `Metadata_*` columns, starting with DeepProfiler and leaving room for DINO-family exporters.

**Architecture:** Add a small generic `cptools2.feature_export` package with a stable export contract and a first DeepProfiler adapter. The Nextflow pipeline will run the exporter after feature extraction, and the same implementation will be exposed through `cptools2 export-features` so existing Eddie runs can be backfilled. CSV is the required artifact; Parquet is optional and config-controlled.

**Tech Stack:** Python stdlib, NumPy for `.npz` inspection, Polars for CSV/Parquet writing, pytest, Nextflow DSL2, Eddie/Singularity runtime.

---

## Phase Boundary

This is a new development phase after Phase 2.8 runtime certification.

Phase 2.8 answers:

```text
Can Cellpose and DeepProfiler run on Eddie GPU nodes and publish native outputs?
```

This phase answers:

```text
Can those native outputs be exported as downstream-ready measurement tables?
```

The phase does not perform normalization, batch correction, well aggregation,
hit calling, UMAP, mechanism-of-action prediction, or feature selection.

## Output Contract

CSV is mandatory. Parquet is optional.

Each successful DeepProfiler export writes:

```text
<output-dir>/tables/deepprofiler_manifest.csv
<output-dir>/tables/deepprofiler_sites.csv
<output-dir>/tables/deepprofiler_cells.csv
```

If Parquet is enabled, the same tables are also written as:

```text
<output-dir>/tables/deepprofiler_manifest.parquet
<output-dir>/tables/deepprofiler_sites.parquet
<output-dir>/tables/deepprofiler_cells.parquet
```

All metadata columns use the `Metadata_*` prefix. Required columns:

```text
Metadata_Plate
Metadata_Well
Metadata_Site
Metadata_ImageSet
Metadata_Object
Metadata_SourceFile
Metadata_FeatureExtractor
```

For site rows, `Metadata_Object` is an empty string. For cell rows, it identifies
the detected object. Prefer the Cellpose `object_id` when available; otherwise
use a 1-based row index from the feature matrix.

Feature columns use extractor-prefixed names:

```text
DeepProfiler_Feature_0000
DeepProfiler_Feature_0001
DeepProfiler_Feature_0002
```

Location/provenance columns that describe object coordinates or masks are still
metadata:

```text
Metadata_Center_X
Metadata_Center_Y
Metadata_MaskPath
```

## File Structure

- Create `cptools2/feature_export/__init__.py`
  - Expose the public export functions and result dataclasses.
- Create `cptools2/feature_export/contract.py`
  - Define shared request/result types, supported output formats, and metadata constants.
- Create `cptools2/feature_export/writers.py`
  - Write CSV and optional Parquet tables from row dictionaries.
- Create `cptools2/feature_export/deepprofiler.py`
  - Inspect DeepProfiler `.npz` files, join metadata, and produce manifest/site/cell rows.
- Modify `cptools2/__main__.py`
  - Add `cptools2 export-features`.
- Modify `cptools2/parse_yaml.py`
  - Parse optional `feature_export` settings and generate Nextflow params.
- Modify `nextflow/main.nf`
  - Wire feature table export after `FEATURE_EXTRACT`.
- Create `nextflow/modules/export_features.nf`
  - Run the CLI exporter in the cptools2 runtime after feature extraction.
- Modify `nextflow/conf/eddie.config`
  - Add conservative CPU/memory resources for the export process.
- Modify `pyproject.toml`
  - Change package discovery from `include = ["cptools2"]` to `include = ["cptools2*"]`.
- Create `tests/test_feature_export_contract.py`
  - Test shared contract validation.
- Create `tests/test_feature_export_writers.py`
  - Test CSV and optional Parquet writing.
- Create `tests/test_feature_export_deepprofiler.py`
  - Test DeepProfiler schema inspection and row generation.
- Modify `tests/test_cli.py`
  - Test the `export-features` CLI parser and command dispatch.
- Modify `tests/test_parse_yaml.py`
  - Test `feature_export` config defaults and param generation.
- Modify `tests/test_nextflow_architecture_smoke.py`
  - Test Nextflow wiring and static command contracts.
- Modify `docs/reference/nextflow-config-yaml.md`
  - Document `feature_export`.
- Modify `docs/reference/deepprofiler-scalability.md`
  - Add a follow-on note that runtime success is not complete until feature tables pass.

---

### Task 1: Add The Shared Feature Export Contract

**Files:**
- Create: `cptools2/feature_export/__init__.py`
- Create: `cptools2/feature_export/contract.py`
- Create: `tests/test_feature_export_contract.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Write failing tests for the export contract**

Create `tests/test_feature_export_contract.py`:

```python
from pathlib import Path

import pytest

from cptools2.feature_export.contract import (
    DEFAULT_FORMATS,
    REQUIRED_METADATA_COLUMNS,
    FeatureExportRequest,
    normalise_formats,
)


def test_required_metadata_columns_use_metadata_prefix():
    assert REQUIRED_METADATA_COLUMNS == (
        "Metadata_Plate",
        "Metadata_Well",
        "Metadata_Site",
        "Metadata_ImageSet",
        "Metadata_Object",
        "Metadata_SourceFile",
        "Metadata_FeatureExtractor",
    )
    assert all(column.startswith("Metadata_") for column in REQUIRED_METADATA_COLUMNS)


def test_normalise_formats_defaults_to_csv_only():
    assert DEFAULT_FORMATS == ("csv",)
    assert normalise_formats(None) == ("csv",)


def test_normalise_formats_accepts_csv_and_parquet_once():
    assert normalise_formats(["CSV", "parquet", "csv"]) == ("csv", "parquet")


def test_normalise_formats_rejects_unknown_format():
    with pytest.raises(ValueError, match="Unsupported feature export format"):
        normalise_formats(["xlsx"])


def test_feature_export_request_creates_tables_dir(tmp_path):
    request = FeatureExportRequest(
        extractor="deepprofiler",
        features_dir=tmp_path / "features",
        output_dir=tmp_path / "exports",
        formats=("csv",),
    )

    assert request.tables_dir == tmp_path / "exports" / "tables"
```

- [ ] **Step 2: Run the contract tests and verify they fail**

Run:

```bash
pytest tests/test_feature_export_contract.py -q
```

Expected:

```text
ModuleNotFoundError: No module named 'cptools2.feature_export'
```

- [ ] **Step 3: Create the feature export package**

Create `cptools2/feature_export/__init__.py`:

```python
"""Feature export helpers for cptools2 analysis outputs."""

from .contract import (
    DEFAULT_FORMATS,
    REQUIRED_METADATA_COLUMNS,
    FeatureExportRequest,
    FeatureExportResult,
    normalise_formats,
)

__all__ = [
    "DEFAULT_FORMATS",
    "REQUIRED_METADATA_COLUMNS",
    "FeatureExportRequest",
    "FeatureExportResult",
    "normalise_formats",
]
```

Create `cptools2/feature_export/contract.py`:

```python
"""Shared contracts for feature table export."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple


DEFAULT_FORMATS = ("csv",)
SUPPORTED_FORMATS = ("csv", "parquet")
REQUIRED_METADATA_COLUMNS = (
    "Metadata_Plate",
    "Metadata_Well",
    "Metadata_Site",
    "Metadata_ImageSet",
    "Metadata_Object",
    "Metadata_SourceFile",
    "Metadata_FeatureExtractor",
)


def normalise_formats(values: Optional[Iterable[str]]) -> Tuple[str, ...]:
    """Return supported output formats in stable order."""

    if values is None:
        return DEFAULT_FORMATS

    requested = {str(value).strip().lower() for value in values if str(value).strip()}
    unsupported = sorted(requested.difference(SUPPORTED_FORMATS))
    if unsupported:
        raise ValueError(
            "Unsupported feature export format: " + ", ".join(unsupported)
        )

    ordered = tuple(value for value in SUPPORTED_FORMATS if value in requested)
    return ordered or DEFAULT_FORMATS


@dataclass(frozen=True)
class FeatureExportRequest:
    extractor: str
    features_dir: Path
    output_dir: Path
    formats: Tuple[str, ...] = DEFAULT_FORMATS
    metadata_dir: Optional[Path] = None
    locations_dir: Optional[Path] = None

    @property
    def tables_dir(self) -> Path:
        return self.output_dir / "tables"


@dataclass(frozen=True)
class FeatureExportResult:
    extractor: str
    manifest_rows: int
    site_rows: int
    cell_rows: int
    written_files: Tuple[Path, ...]
```

- [ ] **Step 4: Update package discovery**

Modify `pyproject.toml`:

```toml
[tool.setuptools.packages.find]
include = ["cptools2*"]
exclude = ["tests", "data", "examples"]
```

This is required because `cptools2/feature_export/` is a subpackage.

- [ ] **Step 5: Run the contract tests and verify they pass**

Run:

```bash
pytest tests/test_feature_export_contract.py -q
```

Expected:

```text
5 passed
```

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml cptools2/feature_export/__init__.py cptools2/feature_export/contract.py tests/test_feature_export_contract.py
git commit -m "feat: add feature export contract"
```

---

### Task 2: Add Table Writers For CSV And Optional Parquet

**Files:**
- Create: `cptools2/feature_export/writers.py`
- Create: `tests/test_feature_export_writers.py`

- [ ] **Step 1: Write failing writer tests**

Create `tests/test_feature_export_writers.py`:

```python
from pathlib import Path

import polars as pl
import pytest

from cptools2.feature_export.writers import write_table


def _rows():
    return [
        {
            "Metadata_Plate": "plate-001",
            "Metadata_Well": "A01",
            "Metadata_Site": "1",
            "Metadata_ImageSet": "plate-001_A01_s1",
            "Metadata_Object": "1",
            "Metadata_SourceFile": "A01-1.npz",
            "Metadata_FeatureExtractor": "deepprofiler",
            "DeepProfiler_Feature_0000": 0.25,
            "DeepProfiler_Feature_0001": 0.75,
        }
    ]


def test_write_table_writes_csv(tmp_path):
    written = write_table(
        rows=_rows(),
        output_stem=tmp_path / "deepprofiler_cells",
        formats=("csv",),
    )

    assert written == (tmp_path / "deepprofiler_cells.csv",)
    df = pl.read_csv(written[0])
    assert df["Metadata_Well"].to_list() == ["A01"]
    assert df["DeepProfiler_Feature_0000"].to_list() == [0.25]


def test_write_table_writes_empty_csv_with_columns(tmp_path):
    columns = [
        "Metadata_Plate",
        "Metadata_Well",
        "Metadata_Site",
        "Metadata_ImageSet",
        "Metadata_Object",
        "Metadata_SourceFile",
        "Metadata_FeatureExtractor",
    ]

    written = write_table(
        rows=[],
        output_stem=tmp_path / "deepprofiler_cells",
        formats=("csv",),
        columns=columns,
    )

    text = Path(written[0]).read_text(encoding="utf-8")
    assert text.startswith("Metadata_Plate,Metadata_Well,Metadata_Site")


def test_write_table_writes_optional_parquet(tmp_path):
    written = write_table(
        rows=_rows(),
        output_stem=tmp_path / "deepprofiler_cells",
        formats=("csv", "parquet"),
    )

    assert written == (
        tmp_path / "deepprofiler_cells.csv",
        tmp_path / "deepprofiler_cells.parquet",
    )
    assert pl.read_parquet(written[1]).height == 1


def test_write_table_requires_columns_for_empty_rows(tmp_path):
    with pytest.raises(ValueError, match="columns are required"):
        write_table(
            rows=[],
            output_stem=tmp_path / "empty",
            formats=("csv",),
        )
```

- [ ] **Step 2: Run the writer tests and verify they fail**

Run:

```bash
pytest tests/test_feature_export_writers.py -q
```

Expected:

```text
ModuleNotFoundError: No module named 'cptools2.feature_export.writers'
```

- [ ] **Step 3: Implement table writing**

Create `cptools2/feature_export/writers.py`:

```python
"""Table writers for feature exports."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Sequence, Tuple

import polars as pl


def write_table(
    rows: Iterable[Mapping[str, object]],
    output_stem: Path,
    formats: Sequence[str],
    columns: Optional[Sequence[str]] = None,
) -> Tuple[Path, ...]:
    """Write rows to the requested formats and return written paths."""

    output_stem = Path(output_stem)
    output_stem.parent.mkdir(parents=True, exist_ok=True)
    row_list = [dict(row) for row in rows]

    if row_list:
        df = pl.DataFrame(row_list)
        if columns is not None:
            df = df.select([column for column in columns if column in df.columns])
    else:
        if columns is None:
            raise ValueError("columns are required when writing an empty table")
        df = pl.DataFrame({column: [] for column in columns})

    written: List[Path] = []
    for fmt in formats:
        if fmt == "csv":
            path = output_stem.with_suffix(".csv")
            df.write_csv(path)
        elif fmt == "parquet":
            path = output_stem.with_suffix(".parquet")
            df.write_parquet(path)
        else:
            raise ValueError(f"Unsupported feature export format: {fmt}")
        written.append(path)
    return tuple(written)
```

- [ ] **Step 4: Run writer tests**

Run:

```bash
pytest tests/test_feature_export_writers.py -q
```

Expected:

```text
4 passed
```

- [ ] **Step 5: Commit**

```bash
git add cptools2/feature_export/writers.py tests/test_feature_export_writers.py
git commit -m "feat: add feature table writers"
```

---

### Task 3: Implement DeepProfiler `.npz` Schema Inspection

**Files:**
- Create: `cptools2/feature_export/deepprofiler.py`
- Create: `tests/test_feature_export_deepprofiler.py`

- [ ] **Step 1: Write failing schema-inspection tests**

Create `tests/test_feature_export_deepprofiler.py`:

```python
import csv

import numpy as np

from cptools2.feature_export.deepprofiler import inspect_npz


def test_inspect_npz_detects_site_vector(tmp_path):
    path = tmp_path / "A01-1.npz"
    np.savez(path, features=np.array([0.1, 0.2, 0.3], dtype="float32"))

    schema = inspect_npz(path)

    assert schema.path == path
    assert schema.kind == "site"
    assert schema.feature_key == "features"
    assert schema.n_rows == 1
    assert schema.n_features == 3


def test_inspect_npz_detects_cell_matrix(tmp_path):
    path = tmp_path / "A01-1.npz"
    np.savez(path, features=np.array([[0.1, 0.2], [0.3, 0.4]], dtype="float32"))

    schema = inspect_npz(path)

    assert schema.kind == "cell"
    assert schema.n_rows == 2
    assert schema.n_features == 2


def test_inspect_npz_uses_single_unnamed_array(tmp_path):
    path = tmp_path / "A01-1.npz"
    np.savez(path, np.array([0.1, 0.2], dtype="float32"))

    schema = inspect_npz(path)

    assert schema.feature_key == "arr_0"
    assert schema.kind == "site"


def test_inspect_npz_reports_unsupported_shapes(tmp_path):
    path = tmp_path / "A01-1.npz"
    np.savez(path, features=np.zeros((2, 2, 2), dtype="float32"))

    schema = inspect_npz(path)

    assert schema.kind == "unsupported"
    assert "Expected 1D or 2D feature array" in schema.reason
```

- [ ] **Step 2: Run schema tests and verify they fail**

Run:

```bash
pytest tests/test_feature_export_deepprofiler.py::test_inspect_npz_detects_site_vector -q
```

Expected:

```text
ImportError: cannot import name 'inspect_npz'
```

- [ ] **Step 3: Implement schema inspection**

Create `cptools2/feature_export/deepprofiler.py` with this initial content:

```python
"""DeepProfiler native-output exporter."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np


FEATURE_KEY_CANDIDATES = ("features", "embeddings", "arr_0")


@dataclass(frozen=True)
class DeepProfilerNpzSchema:
    path: Path
    kind: str
    feature_key: str
    n_rows: int
    n_features: int
    reason: str = ""


def inspect_npz(path: Path) -> DeepProfilerNpzSchema:
    """Inspect a DeepProfiler npz file without exporting rows."""

    path = Path(path)
    with np.load(path, allow_pickle=False) as data:
        feature_key = _select_feature_key(data.files)
        if not feature_key:
            return DeepProfilerNpzSchema(
                path=path,
                kind="unsupported",
                feature_key="",
                n_rows=0,
                n_features=0,
                reason="No supported feature array key found",
            )
        array = data[feature_key]

    if array.ndim == 1:
        return DeepProfilerNpzSchema(
            path=path,
            kind="site",
            feature_key=feature_key,
            n_rows=1,
            n_features=int(array.shape[0]),
        )
    if array.ndim == 2:
        return DeepProfilerNpzSchema(
            path=path,
            kind="cell",
            feature_key=feature_key,
            n_rows=int(array.shape[0]),
            n_features=int(array.shape[1]),
        )
    return DeepProfilerNpzSchema(
        path=path,
        kind="unsupported",
        feature_key=feature_key,
        n_rows=0,
        n_features=0,
        reason=f"Expected 1D or 2D feature array, observed shape {array.shape}",
    )


def _select_feature_key(keys):
    for candidate in FEATURE_KEY_CANDIDATES:
        if candidate in keys:
            return candidate
    if len(keys) == 1:
        return keys[0]
    return ""
```

- [ ] **Step 4: Run schema tests**

Run:

```bash
pytest tests/test_feature_export_deepprofiler.py -q
```

Expected:

```text
4 passed
```

- [ ] **Step 5: Commit**

```bash
git add cptools2/feature_export/deepprofiler.py tests/test_feature_export_deepprofiler.py
git commit -m "feat: inspect DeepProfiler feature schemas"
```

---

### Task 4: Build DeepProfiler Metadata Joins And Row Export

**Files:**
- Modify: `cptools2/feature_export/deepprofiler.py`
- Modify: `tests/test_feature_export_deepprofiler.py`

- [ ] **Step 1: Add tests for site and cell row generation**

Append to `tests/test_feature_export_deepprofiler.py`:

```python
from cptools2.feature_export.contract import FeatureExportRequest
from cptools2.feature_export.deepprofiler import export_deepprofiler


def _write_index(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "Metadata_Plate",
                "Metadata_Well",
                "Metadata_Site",
                "Metadata_ImageSet",
                "DNA",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "Metadata_Plate": "plate-001",
                "Metadata_Well": "A01",
                "Metadata_Site": "1",
                "Metadata_ImageSet": "plate-001_A01_s1",
                "DNA": "plate-001/A01_s1_w1.tif",
            }
        )


def _write_locations(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "plate",
                "well",
                "site",
                "image_set_id",
                "object_id",
                "x",
                "y",
                "mask_path",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "plate": "plate-001",
                "well": "A01",
                "site": "1",
                "image_set_id": "plate-001_A01_s1",
                "object_id": "7",
                "x": "42.5",
                "y": "99.25",
                "mask_path": "masks/plate-001_A01_s1_cp_masks.tif",
            }
        )


def test_export_deepprofiler_writes_site_table_for_site_vector(tmp_path):
    features_dir = tmp_path / "features"
    metadata_dir = tmp_path / "metadata"
    features_dir.mkdir()
    np.savez(features_dir / "A01-1.npz", features=np.array([0.1, 0.2]))
    _write_index(metadata_dir / "index.csv")

    result = export_deepprofiler(
        FeatureExportRequest(
            extractor="deepprofiler",
            features_dir=features_dir,
            output_dir=tmp_path / "export",
            metadata_dir=metadata_dir,
            formats=("csv",),
        )
    )

    assert result.manifest_rows == 1
    assert result.site_rows == 1
    assert result.cell_rows == 0
    site_csv = tmp_path / "export" / "tables" / "deepprofiler_sites.csv"
    rows = list(csv.DictReader(site_csv.open(newline="")))
    assert rows[0]["Metadata_Well"] == "A01"
    assert rows[0]["Metadata_Object"] == ""
    assert rows[0]["DeepProfiler_Feature_0001"] == "0.2"


def test_export_deepprofiler_writes_cell_table_for_cell_matrix(tmp_path):
    features_dir = tmp_path / "features"
    metadata_dir = tmp_path / "metadata"
    locations_dir = tmp_path / "locations"
    features_dir.mkdir()
    np.savez(features_dir / "A01-1.npz", features=np.array([[0.1, 0.2]]))
    _write_index(metadata_dir / "index.csv")
    _write_locations(locations_dir / "chunk_0001_locations.csv")

    result = export_deepprofiler(
        FeatureExportRequest(
            extractor="deepprofiler",
            features_dir=features_dir,
            output_dir=tmp_path / "export",
            metadata_dir=metadata_dir,
            locations_dir=locations_dir,
            formats=("csv",),
        )
    )

    assert result.site_rows == 1
    assert result.cell_rows == 1
    rows = list(
        csv.DictReader(
            (tmp_path / "export" / "tables" / "deepprofiler_cells.csv").open(
                newline=""
            )
        )
    )
    assert rows[0]["Metadata_Object"] == "7"
    assert rows[0]["Metadata_Center_X"] == "42.5"
    assert rows[0]["Metadata_MaskPath"] == "masks/plate-001_A01_s1_cp_masks.tif"


def test_export_deepprofiler_manifest_records_unsupported_npz(tmp_path):
    features_dir = tmp_path / "features"
    metadata_dir = tmp_path / "metadata"
    features_dir.mkdir()
    np.savez(features_dir / "A01-1.npz", features=np.zeros((1, 1, 1)))
    _write_index(metadata_dir / "index.csv")

    result = export_deepprofiler(
        FeatureExportRequest(
            extractor="deepprofiler",
            features_dir=features_dir,
            output_dir=tmp_path / "export",
            metadata_dir=metadata_dir,
            formats=("csv",),
        )
    )

    assert result.manifest_rows == 1
    assert result.site_rows == 0
    manifest_rows = list(
        csv.DictReader(
            (tmp_path / "export" / "tables" / "deepprofiler_manifest.csv").open(
                newline=""
            )
        )
    )
    assert manifest_rows[0]["Metadata_ExportStatus"] == "unsupported"
```

- [ ] **Step 2: Run the new row-generation tests and verify they fail**

Run:

```bash
pytest tests/test_feature_export_deepprofiler.py -q
```

Expected:

```text
ImportError: cannot import name 'export_deepprofiler'
```

- [ ] **Step 3: Implement DeepProfiler export helpers**

Append the following implementation to `cptools2/feature_export/deepprofiler.py`:

```python
import csv
from collections import defaultdict
from typing import Dict, List, Mapping, Optional

from .contract import FeatureExportRequest, FeatureExportResult
from .writers import write_table


MANIFEST_COLUMNS = (
    "Metadata_Plate",
    "Metadata_Well",
    "Metadata_Site",
    "Metadata_ImageSet",
    "Metadata_SourceFile",
    "Metadata_FeatureExtractor",
    "Metadata_ExportStatus",
    "Metadata_NpzKind",
    "Metadata_FeatureKey",
    "Metadata_RowCount",
    "Metadata_FeatureCount",
    "Metadata_Reason",
)


def export_deepprofiler(request: FeatureExportRequest) -> FeatureExportResult:
    """Export DeepProfiler native npz outputs to measurement tables."""

    metadata = _read_metadata_index(request.metadata_dir)
    locations = _read_locations(request.locations_dir)
    manifest_rows = []
    site_rows = []
    cell_rows = []

    for path in sorted(Path(request.features_dir).rglob("*.npz")):
        schema = inspect_npz(path)
        metadata_row = _match_metadata(path, metadata)
        status = "exported" if schema.kind in {"site", "cell"} and metadata_row else "unsupported"
        reason = schema.reason
        if not metadata_row:
            status = "unmatched"
            reason = "No metadata row matched npz file"
        manifest_rows.append(_manifest_row(schema, metadata_row, status, reason))
        if status != "exported":
            continue

        with np.load(path, allow_pickle=False) as data:
            values = data[schema.feature_key]

        site_rows.append(_site_row(path, metadata_row, schema, values))
        if schema.kind == "cell":
            cell_rows.extend(
                _cell_rows(
                    path=path,
                    metadata_row=metadata_row,
                    values=values,
                    locations=locations.get(metadata_row["Metadata_ImageSet"], []),
                )
            )

    written = []
    written.extend(
        write_table(
            manifest_rows,
            request.tables_dir / "deepprofiler_manifest",
            request.formats,
            columns=MANIFEST_COLUMNS,
        )
    )
    written.extend(
        write_table(
            site_rows,
            request.tables_dir / "deepprofiler_sites",
            request.formats,
            columns=_measurement_columns(site_rows),
        )
    )
    written.extend(
        write_table(
            cell_rows,
            request.tables_dir / "deepprofiler_cells",
            request.formats,
            columns=_measurement_columns(cell_rows),
        )
    )

    return FeatureExportResult(
        extractor="deepprofiler",
        manifest_rows=len(manifest_rows),
        site_rows=len(site_rows),
        cell_rows=len(cell_rows),
        written_files=tuple(written),
    )


def _read_metadata_index(metadata_dir: Optional[Path]) -> List[Dict[str, str]]:
    if metadata_dir is None:
        return []
    path = Path(metadata_dir) / "index.csv"
    if not path.exists():
        return []
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def _read_locations(locations_dir: Optional[Path]) -> Dict[str, List[Dict[str, str]]]:
    grouped = defaultdict(list)
    if locations_dir is None:
        return grouped
    for path in Path(locations_dir).rglob("*.csv"):
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                image_set = row.get("image_set_id") or _image_set_from_location(row)
                grouped[image_set].append(row)
    return grouped


def _image_set_from_location(row: Mapping[str, str]) -> str:
    return "{}_{}_s{}".format(row.get("plate", ""), row.get("well", ""), row.get("site", ""))


def _match_metadata(path: Path, rows: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    stem = path.stem
    for row in rows:
        if row.get("Metadata_ImageSet") == stem:
            return row
    for row in rows:
        well = row.get("Metadata_Well", "")
        site = row.get("Metadata_Site", "")
        accepted = {
            f"{well}-{site}",
            f"{well}-f{site}",
            f"{well}-f{int(site):02d}" if str(site).isdigit() else f"{well}-f{site}",
        }
        if stem in accepted:
            return row
    return rows[0] if len(rows) == 1 else None


def _base_metadata(path: Path, row: Mapping[str, str], object_id: str) -> Dict[str, object]:
    return {
        "Metadata_Plate": row.get("Metadata_Plate", ""),
        "Metadata_Well": row.get("Metadata_Well", ""),
        "Metadata_Site": row.get("Metadata_Site", ""),
        "Metadata_ImageSet": row.get("Metadata_ImageSet", ""),
        "Metadata_Object": object_id,
        "Metadata_SourceFile": path.as_posix(),
        "Metadata_FeatureExtractor": "deepprofiler",
    }


def _feature_values(values) -> Dict[str, float]:
    flat = np.asarray(values, dtype=float).reshape(-1)
    return {
        f"DeepProfiler_Feature_{idx:04d}": float(value)
        for idx, value in enumerate(flat)
    }


def _site_row(path: Path, row: Mapping[str, str], schema: DeepProfilerNpzSchema, values) -> Dict[str, object]:
    feature_values = values.mean(axis=0) if schema.kind == "cell" else values
    output = _base_metadata(path, row, "")
    output.update(_feature_values(feature_values))
    return output


def _cell_rows(path: Path, metadata_row: Mapping[str, str], values, locations: List[Dict[str, str]]) -> List[Dict[str, object]]:
    rows = []
    for idx, vector in enumerate(values):
        location = locations[idx] if idx < len(locations) else {}
        object_id = location.get("object_id") or str(idx + 1)
        output = _base_metadata(path, metadata_row, object_id)
        output.update(
            {
                "Metadata_Center_X": location.get("x", ""),
                "Metadata_Center_Y": location.get("y", ""),
                "Metadata_MaskPath": location.get("mask_path", ""),
            }
        )
        output.update(_feature_values(vector))
        rows.append(output)
    return rows


def _manifest_row(schema: DeepProfilerNpzSchema, metadata_row: Optional[Mapping[str, str]], status: str, reason: str) -> Dict[str, object]:
    metadata_row = metadata_row or {}
    return {
        "Metadata_Plate": metadata_row.get("Metadata_Plate", ""),
        "Metadata_Well": metadata_row.get("Metadata_Well", ""),
        "Metadata_Site": metadata_row.get("Metadata_Site", ""),
        "Metadata_ImageSet": metadata_row.get("Metadata_ImageSet", ""),
        "Metadata_SourceFile": schema.path.as_posix(),
        "Metadata_FeatureExtractor": "deepprofiler",
        "Metadata_ExportStatus": status,
        "Metadata_NpzKind": schema.kind,
        "Metadata_FeatureKey": schema.feature_key,
        "Metadata_RowCount": schema.n_rows,
        "Metadata_FeatureCount": schema.n_features,
        "Metadata_Reason": reason,
    }


def _measurement_columns(rows: List[Dict[str, object]]) -> List[str]:
    base = [
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
    ]
    feature_columns = sorted(
        {
            column
            for row in rows
            for column in row
            if column.startswith("DeepProfiler_Feature_")
        }
    )
    return base + feature_columns
```

- [ ] **Step 4: Run DeepProfiler export tests**

Run:

```bash
pytest tests/test_feature_export_deepprofiler.py -q
```

Expected:

```text
7 passed
```

- [ ] **Step 5: Commit**

```bash
git add cptools2/feature_export/deepprofiler.py tests/test_feature_export_deepprofiler.py
git commit -m "feat: export DeepProfiler feature tables"
```

---

### Task 5: Add The `cptools2 export-features` CLI

**Files:**
- Modify: `cptools2/__main__.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Add failing CLI parser tests**

Add to `tests/test_cli.py`:

```python
def test_export_features_parser_defaults_to_csv():
    parser = cli.build_parser()
    args = parser.parse_args(
        [
            "export-features",
            "/tmp/run-output/features",
            "--extractor",
            "deepprofiler",
            "--output-dir",
            "/tmp/run-output",
            "--metadata-dir",
            "/tmp/dp_project/inputs/metadata",
        ]
    )

    assert args.command == "export-features"
    assert args.extractor == "deepprofiler"
    assert args.formats == ["csv"]


def test_export_features_parser_accepts_parquet_as_optional_format():
    parser = cli.build_parser()
    args = parser.parse_args(
        [
            "export-features",
            "/tmp/features",
            "--extractor",
            "deepprofiler",
            "--output-dir",
            "/tmp/output",
            "--formats",
            "csv",
            "parquet",
        ]
    )

    assert args.formats == ["csv", "parquet"]
```

- [ ] **Step 2: Run CLI tests and verify they fail**

Run:

```bash
pytest tests/test_cli.py::test_export_features_parser_defaults_to_csv tests/test_cli.py::test_export_features_parser_accepts_parquet_as_optional_format -q
```

Expected:

```text
argparse.ArgumentError: invalid choice: 'export-features'
```

- [ ] **Step 3: Implement CLI dispatch**

In `cptools2/__main__.py`, add imports near the existing imports:

```python
from pathlib import Path

from cptools2.feature_export.contract import FeatureExportRequest, normalise_formats
from cptools2.feature_export.deepprofiler import export_deepprofiler
```

Add this command function near the existing command functions:

```python
def cmd_export_features(args):
    formats = normalise_formats(args.formats)
    request = FeatureExportRequest(
        extractor=args.extractor,
        features_dir=Path(args.features_dir),
        output_dir=Path(args.output_dir),
        metadata_dir=Path(args.metadata_dir) if args.metadata_dir else None,
        locations_dir=Path(args.locations_dir) if args.locations_dir else None,
        formats=formats,
    )
    if args.extractor == "deepprofiler":
        result = export_deepprofiler(request)
    else:
        raise ValueError(f"Unsupported feature extractor: {args.extractor}")

    print(
        "Exported {extractor}: manifest={manifest} sites={sites} cells={cells}".format(
            extractor=result.extractor,
            manifest=result.manifest_rows,
            sites=result.site_rows,
            cells=result.cell_rows,
        )
    )
```

Add this parser block before the deprecated `generate` parser:

```python
    # --- export-features ---
    p_export = subparsers.add_parser(
        "export-features",
        help="export native feature outputs to measurement tables",
    )
    p_export.add_argument("features_dir", help="directory containing native feature outputs")
    p_export.add_argument(
        "--extractor",
        choices=["deepprofiler"],
        required=True,
        help="feature extractor that produced the native outputs",
    )
    p_export.add_argument(
        "--output-dir",
        required=True,
        help="directory where feature tables should be written",
    )
    p_export.add_argument(
        "--metadata-dir",
        default=None,
        help="directory containing metadata/index.csv",
    )
    p_export.add_argument(
        "--locations-dir",
        default=None,
        help="directory containing Cellpose location CSVs",
    )
    p_export.add_argument(
        "--formats",
        nargs="+",
        default=["csv"],
        help="output formats: csv or csv parquet",
    )
    p_export.set_defaults(func=cmd_export_features)
```

- [ ] **Step 4: Run CLI tests**

Run:

```bash
pytest tests/test_cli.py::test_export_features_parser_defaults_to_csv tests/test_cli.py::test_export_features_parser_accepts_parquet_as_optional_format -q
```

Expected:

```text
2 passed
```

- [ ] **Step 5: Commit**

```bash
git add cptools2/__main__.py tests/test_cli.py
git commit -m "feat: add feature export CLI"
```

---

### Task 6: Add Config And Param Support For Feature Export

**Files:**
- Modify: `cptools2/parse_yaml.py`
- Modify: `tests/test_parse_yaml.py`
- Modify: `docs/reference/nextflow-config-yaml.md`

- [ ] **Step 1: Add failing config tests**

Add to `tests/test_parse_yaml.py`:

```python
def test_generate_params_json_defaults_feature_export_to_csv(tmp_path):
    config = parse_yaml.parse_config_file(PIPELINE_CONFIG_PATH)
    config["feature_extraction"] = {"tool": "deepprofiler"}

    params = parse_yaml.generate_params_json(config, tmp_path / "params.json")

    assert params["feature_export_enabled"] is True
    assert params["feature_export_formats"] == "csv"


def test_generate_params_json_accepts_feature_export_formats(tmp_path):
    config = parse_yaml.parse_config_file(PIPELINE_CONFIG_PATH)
    config["feature_extraction"] = {"tool": "deepprofiler"}
    config["feature_export"] = {
        "enabled": True,
        "formats": ["csv", "parquet"],
    }

    params = parse_yaml.generate_params_json(config, tmp_path / "params.json")

    assert params["feature_export_enabled"] is True
    assert params["feature_export_formats"] == "csv,parquet"


def test_generate_params_json_can_disable_feature_export(tmp_path):
    config = parse_yaml.parse_config_file(PIPELINE_CONFIG_PATH)
    config["feature_extraction"] = {"tool": "deepprofiler"}
    config["feature_export"] = {"enabled": False}

    params = parse_yaml.generate_params_json(config, tmp_path / "params.json")

    assert params["feature_export_enabled"] is False
```

- [ ] **Step 2: Run config tests and verify they fail**

Run:

```bash
pytest tests/test_parse_yaml.py::test_generate_params_json_defaults_feature_export_to_csv tests/test_parse_yaml.py::test_generate_params_json_accepts_feature_export_formats tests/test_parse_yaml.py::test_generate_params_json_can_disable_feature_export -q
```

Expected:

```text
KeyError: 'feature_export_enabled'
```

- [ ] **Step 3: Implement param generation**

In `cptools2/parse_yaml.py`, inside `generate_params_json`, add:

```python
    feature_extraction = yaml_dict.get("feature_extraction")
    feature_export = yaml_dict.get("feature_export") or {}
    export_enabled = bool(feature_extraction) and feature_export.get("enabled", True)
    params["feature_export_enabled"] = export_enabled
    formats = feature_export.get("formats", ["csv"])
    if isinstance(formats, str):
        formats = [formats]
    params["feature_export_formats"] = ",".join(str(value).lower() for value in formats)
```

Place this near the existing `feature_extraction` param block so export settings stay close to feature extraction settings.

- [ ] **Step 4: Run config tests**

Run:

```bash
pytest tests/test_parse_yaml.py::test_generate_params_json_defaults_feature_export_to_csv tests/test_parse_yaml.py::test_generate_params_json_accepts_feature_export_formats tests/test_parse_yaml.py::test_generate_params_json_can_disable_feature_export -q
```

Expected:

```text
3 passed
```

- [ ] **Step 5: Document config**

In `docs/reference/nextflow-config-yaml.md`, add after the `feature_extraction` section:

```markdown
### `feature_export`

Feature export controls whether native feature extractor outputs are converted
to downstream-ready measurement tables.

```yaml
feature_export:
  enabled: true
  formats:
    - csv
```

CSV is the required format. Parquet can be requested as an additional format:

```yaml
feature_export:
  enabled: true
  formats:
    - csv
    - parquet
```

Feature export tables use `Metadata_*` columns for plate, well, site, image set,
object identity, source file, and feature extractor provenance.
```

- [ ] **Step 6: Commit**

```bash
git add cptools2/parse_yaml.py tests/test_parse_yaml.py docs/reference/nextflow-config-yaml.md
git commit -m "feat: configure feature table export"
```

---

### Task 7: Wire Feature Export Into Nextflow

**Files:**
- Create: `nextflow/modules/export_features.nf`
- Modify: `nextflow/main.nf`
- Modify: `nextflow/modules/feature_extract.nf`
- Modify: `nextflow/conf/eddie.config`
- Modify: `tests/test_nextflow_architecture_smoke.py`

- [ ] **Step 1: Add failing architecture tests**

Add to `tests/test_nextflow_architecture_smoke.py`:

```python
def test_nextflow_includes_feature_export_module():
    main_nf = (ROOT / "nextflow" / "main.nf").read_text()

    assert "include { EXPORT_FEATURES } from './modules/export_features'" in main_nf
    assert "params.feature_export_enabled" in main_nf
    assert "EXPORT_FEATURES(" in main_nf


def test_export_features_module_uses_cptools2_cli():
    module = (ROOT / "nextflow" / "modules" / "export_features.nf").read_text()

    assert "process EXPORT_FEATURES" in module
    assert "python -m cptools2.__main__ export-features" in module
    assert "--extractor ${params.feature_extraction_tool}" in module
    assert "--formats" in module
    assert "deepprofiler_manifest.csv" in module


def test_feature_extract_emits_export_inputs():
    module = (ROOT / "nextflow" / "modules" / "feature_extract.nf").read_text()

    assert "emit: export_inputs" in module
    assert "feature_export_metadata" in module
    assert "feature_export_locations" in module
```

- [ ] **Step 2: Run architecture tests and verify they fail**

Run:

```bash
pytest tests/test_nextflow_architecture_smoke.py::test_nextflow_includes_feature_export_module tests/test_nextflow_architecture_smoke.py::test_export_features_module_uses_cptools2_cli -q
```

Expected:

```text
FileNotFoundError: export_features.nf
```

- [ ] **Step 3: Add the Nextflow export module**

Create `nextflow/modules/export_features.nf`:

```nextflow
// Export native feature outputs to downstream-ready measurement tables.

process EXPORT_FEATURES {
    tag "${plate_id}:${features_dir.simpleName}"
    label 'feature_export'

    publishDir "${params.output_dir}/${plate_id}/features/${features_dir.simpleName}/tables", mode: 'copy'

    input:
    tuple val(plate_id), path(features_dir), path(metadata_dir), path(locations_dir)

    output:
    tuple val(plate_id), path("tables"), emit: tables

    script:
    """
    mkdir -p tables
    export PYTHONPATH="\${CPTOOLS2_PROJECT_ROOT}:\${PYTHONPATH:-}"

    python -m cptools2.__main__ export-features ${features_dir} \\
        --extractor ${params.feature_extraction_tool} \\
        --output-dir . \\
        --metadata-dir ${metadata_dir} \\
        --locations-dir ${locations_dir} \\
        --formats ${params.feature_export_formats.replace(',', ' ')}

    test -s tables/deepprofiler_manifest.csv
    test -s tables/deepprofiler_sites.csv
    test -f tables/deepprofiler_cells.csv
    """
}
```

If Nextflow string replacement is rejected by the DSL parser, replace the script
line with an environment variable:

```nextflow
        --formats \$(printf '%s' "${params.feature_export_formats}" | tr ',' ' ')
```

- [ ] **Step 4: Extend `FEATURE_EXTRACT` outputs without breaking existing staging**

In `nextflow/modules/feature_extract.nf`, keep the existing `features` output and
add a second output for export:

```nextflow
    output:
    tuple val(plate_id), path("features"), emit: features
    tuple val(plate_id), path("features"), path("feature_export_metadata"), path("feature_export_locations"), emit: export_inputs
```

Near the top of the DeepProfiler script block, create the export directories:

```bash
        mkdir -p features
        mkdir -p feature_export_metadata feature_export_locations
```

After `python -m cptools2.nextflow_chunking deepprofiler-package ...` succeeds,
copy the package metadata into stable process outputs:

```bash
        cp -r dp_project/inputs/metadata/* feature_export_metadata/
        cp -r dp_project/inputs/locations/* feature_export_locations/
```

For the non-DeepProfiler branch, create empty export directories so the declared
output contract is still satisfied:

```bash
        mkdir -p feature_export_metadata feature_export_locations
```

The `FEATURE_EXTRACT.out.features` channel must remain unchanged so existing
`STAGE_OUT(FEATURE_EXTRACT.out.features)` behavior continues to work.

- [ ] **Step 5: Wire the module in `nextflow/main.nf`**

Add near the other includes:

```nextflow
include { EXPORT_FEATURES } from './modules/export_features'
```

Add defaults near feature extraction params:

```nextflow
params.feature_export_enabled = true
params.feature_export_formats = 'csv'
```

After each `FEATURE_EXTRACT(...)` call, invoke export only for DeepProfiler when
export is enabled:

```nextflow
if (params.feature_export_enabled && params.feature_extraction_tool.toString().toLowerCase() == 'deepprofiler') {
    EXPORT_FEATURES(FEATURE_EXTRACT.out.export_inputs)
    STAGE_OUT(EXPORT_FEATURES.out.tables)
}
```

Keep the existing later `STAGE_OUT(FEATURE_EXTRACT.out.features)` call. Native
feature outputs and feature table outputs should both be destaged when
`params.stage_data` is true.

- [ ] **Step 6: Add Eddie resource policy**

In `nextflow/conf/eddie.config`, add a CPU-only process label:

```groovy
    withLabel: 'feature_export' {
        queue = cptools2CpuQueue
        cpus = 1
        memory = '8 GB'
        time = '02:00:00'
    }
```

Feature export should not request GPU resources.

- [ ] **Step 7: Run architecture tests**

Run:

```bash
pytest tests/test_nextflow_architecture_smoke.py::test_nextflow_includes_feature_export_module tests/test_nextflow_architecture_smoke.py::test_export_features_module_uses_cptools2_cli tests/test_nextflow_architecture_smoke.py::test_feature_extract_emits_export_inputs -q
```

Expected:

```text
3 passed
```

- [ ] **Step 8: Commit**

```bash
git add nextflow/modules/export_features.nf nextflow/modules/feature_extract.nf nextflow/main.nf nextflow/conf/eddie.config tests/test_nextflow_architecture_smoke.py
git commit -m "feat: export feature tables in Nextflow"
```

---

### Task 8: Add Backfill And Evidence Commands For Eddie

**Files:**
- Modify: `docs/reference/deepprofiler-scalability.md`
- Modify: `docs/superpowers/plans/2026-05-19-feature-export-tables-measurement-contracts.md`

- [ ] **Step 1: Backfill the existing full-plate route when Eddie exec works**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 && python3 -m cptools2.__main__ export-features /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency8/outputs/sarah-representative/features --extractor deepprofiler --output-dir /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency8/outputs/sarah-representative/features --metadata-dir /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency8/outputs/work/batch_001 --locations-dir /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency8/outputs/sarah-representative/locations --formats csv"
```

If the metadata or locations directory above is not correct for the completed
run, locate them with:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "find /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency8 -path '*/metadata/index.csv' -o -path '*locations*.csv' | head -100"
```

- [ ] **Step 2: Validate table outputs on Eddie**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "python3 - <<'PY'
import csv
from pathlib import Path
root = Path('/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency8/outputs/sarah-representative/features/tables')
for name in ['deepprofiler_manifest.csv', 'deepprofiler_sites.csv', 'deepprofiler_cells.csv']:
    path = root / name
    print(name, 'exists', path.exists(), 'size', path.stat().st_size if path.exists() else 0)
    if path.exists():
        with path.open(newline='') as handle:
            reader = csv.DictReader(handle)
            rows = sum(1 for _ in reader)
            print(name, 'rows', rows, 'columns', len(reader.fieldnames or []))
            print(name, 'metadata_columns', [c for c in (reader.fieldnames or []) if c.startswith('Metadata_')][:12])
PY"
```

Expected for the full-plate route:

```text
deepprofiler_manifest.csv exists True
deepprofiler_sites.csv exists True
deepprofiler_cells.csv exists True
```

The row counts depend on the actual DeepProfiler `.npz` schema:

- site-vector schema: `deepprofiler_sites.csv` should have 384 rows and `deepprofiler_cells.csv` should have 0 rows plus headers.
- cell-matrix schema: `deepprofiler_sites.csv` should have 384 rows and `deepprofiler_cells.csv` should have one row per detected object.

- [ ] **Step 3: Document evidence**

Add this section to `docs/reference/deepprofiler-scalability.md` after the full
plate scalability gate:

```markdown
## Feature Table Export Gate

Runtime success is not the final usability gate. A DeepProfiler run is complete
for downstream analysis only when native `.npz` outputs have been exported into
CSV measurement tables with `Metadata_*` columns.

Required files:

```text
deepprofiler_manifest.csv
deepprofiler_sites.csv
deepprofiler_cells.csv
```

The first full-plate backfill target is:

```text
/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency8
```
```

- [ ] **Step 4: Commit docs**

```bash
git add docs/reference/deepprofiler-scalability.md docs/superpowers/plans/2026-05-19-feature-export-tables-measurement-contracts.md
git commit -m "docs: add feature export evidence gate"
```

---

### Task 9: Run Focused Verification

**Files:**
- No new files.

- [ ] **Step 1: Run feature export tests**

Run:

```bash
pytest tests/test_feature_export_contract.py tests/test_feature_export_writers.py tests/test_feature_export_deepprofiler.py -q
```

Expected:

```text
16 passed
```

- [ ] **Step 2: Run integration-adjacent tests**

Run:

```bash
pytest tests/test_cli.py tests/test_parse_yaml.py tests/test_nextflow_architecture_smoke.py -q
```

Expected:

```text
all selected tests pass
```

- [ ] **Step 3: Run Eddie runtime config tests**

Run:

```bash
pytest tests/test_eddie_runtime_config.py -q
```

Expected:

```text
all selected tests pass
```

- [ ] **Step 4: Run the full local test suite if time allows**

Run:

```bash
pytest -q
```

Expected:

```text
all tests pass
```

- [ ] **Step 5: Commit any test fixes**

If the verification run reveals necessary fixes in the feature export scope,
commit the exact files that changed. For this plan, expected paths are:

```bash
git status --short
git add cptools2/feature_export cptools2/__main__.py cptools2/parse_yaml.py nextflow/main.nf nextflow/modules/export_features.nf nextflow/conf/eddie.config tests/test_feature_export_contract.py tests/test_feature_export_writers.py tests/test_feature_export_deepprofiler.py tests/test_cli.py tests/test_parse_yaml.py tests/test_nextflow_architecture_smoke.py
git commit -m "test: stabilize feature export coverage"
```

---

## Engineering Review Notes

Key risks to check before execution:

- **BLOCKER:** `EXPORT_FEATURES` must produce one plate-level table set, not one
  uncoordinated table set per chunk. The implementation should either group
  `FEATURE_EXTRACT.out.export_inputs` by `plate_id` before export, or add a
  follow-on `MERGE_FEATURE_TABLES` process. Without this, every chunk writes
  `deepprofiler_sites.csv` and `deepprofiler_cells.csv`, which creates collision
  risk and leaves users without a single full-plate table.
- The real DeepProfiler `.npz` schema may not use `features`, `embeddings`, or `arr_0`. The manifest must make this visible instead of silently producing empty tables.
- `nextflow/main.nf` currently passes `FEATURE_EXTRACT` a tuple containing corrected images, chunk manifest, and locations. `EXPORT_FEATURES` needs access to metadata generated inside each `FEATURE_EXTRACT` task. If that metadata is not emitted, the feature extraction process should emit the package metadata directory as a second output.
- Table export should remain CPU-only. It should not consume GPU queue slots.
- `Metadata_*` names are the stable public contract. Avoid adding unprefixed `plate`, `well`, `site`, or `object_id` columns to exported tables.
- Empty cell tables are valid. They must contain headers and zero rows, not be missing.
- Empty/no-cell sites still need a row in `deepprofiler_sites.csv`. The exporter
  should create a metadata-only site row with `Metadata_ExportStatus=no_cells`
  when a site has metadata and locations but no feature vector.
- CSV is required. Parquet must never be the only emitted format.

## Plan Engineering Review Outcome

**Verdict: APPROVED WITH REQUIRED REVISION BEFORE EXECUTION**

The architecture direction is right: generic export contract, DeepProfiler first
adapter, CSV as the required artifact, optional Parquet, pipeline plus CLI, and
`Metadata_*` columns. The plan should not be executed exactly as written until
Task 7 is revised to make export a plate-level artifact. Chunk-local CSVs are
useful as intermediates, but the user-facing deliverable must be a single
full-plate `deepprofiler_sites.csv`, `deepprofiler_cells.csv`, and
`deepprofiler_manifest.csv`.

## Self-Review

**Spec coverage:** The plan covers the approved decisions: both site and cell outputs with fallback, CSV required and Parquet optional, pipeline plus CLI entry points, `Metadata_*` metadata columns, DeepProfiler first adapter, and a generic export package for future DINO adapters.

**Placeholder scan:** The plan intentionally contains no TBD/TODO placeholders. The one adaptive instruction is in Task 7, where the executor must inspect the current `main.nf` channel names before wiring `EXPORT_FEATURES`; this is necessary because the pipeline graph is already evolving in the dirty worktree.

**Type consistency:** The planned public objects are `FeatureExportRequest`, `FeatureExportResult`, `normalise_formats`, `write_table`, `inspect_npz`, and `export_deepprofiler`. The same names are used consistently in tests, CLI dispatch, and Nextflow integration.
