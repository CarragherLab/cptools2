# DINOv2 Phase Feature Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build baseline DINOv2 whole-image feature extraction for IncuCyte S3 10X phase/brightfield TIFF images in the cptools2 Nextflow pipeline.

**Architecture:** Add a small IncuCyte manifest adapter, a DINO-family embedding CLI with a DINOv2 implementation, and a Nextflow `FEATURE_EXTRACT` branch that produces one Parquet embedding row per TIFF image. Keep this branch-scoped to `ai-update-DINO` and do not promote it into the default project loop queue until Eddie tiny-smoke evidence exists.

**Tech Stack:** Python 3.8+, pytest, polars, PyYAML, numpy, Pillow, pyarrow, PyTorch inside the DINOv2 container, Nextflow DSL2, Singularity on Eddie.

---

## Scope

This plan implements the branch-scoped DINOv2 baseline described in `.claude/plans/phase-2.9-dinov2-phase-feature-extraction.md`.

Build exactly this first:

- IncuCyte S3 filename parsing for the observed `DSeqPwCNS_*` phase/brightfield TIFF export shape.
- One embedding row per whole TIFF image.
- Nullable `timepoint` output column, emitted for every IncuCyte-derived row.
- `phase_image_path` as the canonical manifest image path column.
- `dinov2_vitl14` as the primary model target.
- Expected weights path: `${CPTOOLS2_MODEL_DIR}/dinov2/dinov2_vitl14.pth`.
- Segment-free DINOv2 extraction. Cellpose is not required for this baseline.

Do not build in this plan:

- Tile-level embeddings.
- LoRA fine-tuning.
- Cell-DINO or uniDINO implementation.
- Downstream aggregation, similarity search, UMAP, hit calling, or MoA prediction.
- Default project-queue promotion.

## File Structure

Create:

- `cptools2/incucyte.py`: IncuCyte filename parsing and manifest discovery/writing.
- `cptools2/dino_embed.py`: DINO-family embedding CLI and reusable extractor logic.
- `tests/test_incucyte.py`: parser and manifest tests for IncuCyte examples.
- `tests/test_dino_embed.py`: local extractor tests using a constant adapter.
- `config/dinov2-phase-smoke.yaml`: branch-scoped smoke config for the agreed IncuCyte subset.
- `cptools2/dockerfiles/Dockerfile.dinov2`: GPU runtime for DINOv2 inference.
- `scripts/eddie_smoke_dinov2.sh`: Eddie GPU model-load smoke script.

Modify:

- `pyproject.toml`: add lightweight runtime dependencies needed by the local extractor.
- `cptools2/parse_yaml.py`: pass DINOv2-specific params, allow segment-free phase DINOv2, resolve DINOv2 container role.
- `cptools2/containers.py`: add `dinov2` default role.
- `cptools2/container_manifest_template.json`: add DINOv2 container metadata.
- `cptools2/dockerfiles/build_containers.sh`: build `dinov2_1.0.sif` from `dinov2_1.0.tar`.
- `cptools2/dockerfiles/README.md`: document local Docker build, archive, SIF conversion, and validation.
- `nextflow/modules/feature_extract.nf`: replace DINOv2 placeholder with `cptools2.dino_embed`.
- `nextflow/main.nf`: allow extract-only DINOv2 phase runs without segmentation.
- `nextflow/conf/containers.config`: keep Docker image name aligned.
- `nextflow/conf/eddie.config`: point `dinov2` at `${CPTOOLS2_CONTAINER_DIR}/dinov2_1.0.sif`.
- `tests/test_parse_yaml.py`: DINOv2 params and segmentation-contract tests.
- `tests/test_containers.py`: DINOv2 role and manifest tests.
- `tests/test_nextflow_architecture_smoke.py`: DINOv2 Nextflow branch tests.
- `tests/test_eddie_runtime_config.py`: DINOv2 container/build/smoke documentation tests.
- `docs/reference/nextflow-config-yaml.md`: document IncuCyte DINOv2 config keys.

High-conflict files:

- `nextflow/modules/feature_extract.nf`
- `nextflow/main.nf`
- `nextflow/conf/eddie.config`
- `cptools2/parse_yaml.py`

Before merging this branch back to `ai-update`, rebase against current Phase 2.8 and rerun the DeepProfiler tests listed in Task 10.

---

### Task 1: IncuCyte Filename Parser and Manifest

**Files:**
- Create: `cptools2/incucyte.py`
- Create: `tests/test_incucyte.py`

- [ ] **Step 1: Write parser tests**

Create `tests/test_incucyte.py` with:

```python
from pathlib import Path

import pytest

from cptools2.incucyte import (
    IncuCyteParseError,
    discover_incucyte_images,
    parse_incucyte_filename,
    write_incucyte_manifest,
)


def test_parse_incucyte_filename_with_timepoint():
    parsed = parse_incucyte_filename("DSeqPwCNS_1_A10_1_01d01h16m.tif")

    assert parsed == {
        "plate_name": "DSeqPwCNS_1",
        "well": "A10",
        "site": "1",
        "timepoint": "01d01h16m",
        "filename": "DSeqPwCNS_1_A10_1_01d01h16m.tif",
    }


def test_parse_incucyte_filename_without_timepoint():
    parsed = parse_incucyte_filename("DSeqPwCNS_10_B2_1.tif")

    assert parsed == {
        "plate_name": "DSeqPwCNS_10",
        "well": "B2",
        "site": "1",
        "timepoint": None,
        "filename": "DSeqPwCNS_10_B2_1.tif",
    }


def test_parse_incucyte_filename_rejects_bad_name():
    with pytest.raises(IncuCyteParseError, match="Cannot parse IncuCyte TIFF"):
        parse_incucyte_filename("not_an_incucyte_file.tif")


def test_discover_incucyte_images_preserves_paths(tmp_path):
    plate = tmp_path / "DSeqPwCNS_1"
    plate.mkdir()
    image = plate / "DSeqPwCNS_1_A1_1_00d00h00m.tif"
    image.write_bytes(b"fake-tiff-placeholder")

    rows = discover_incucyte_images(tmp_path)

    assert rows == [
        {
            "plate_name": "DSeqPwCNS_1",
            "well": "A1",
            "site": "1",
            "timepoint": "00d00h00m",
            "filename": "DSeqPwCNS_1_A1_1_00d00h00m.tif",
            "phase_image_path": str(image),
        }
    ]


def test_write_incucyte_manifest_writes_stable_columns(tmp_path):
    plate = tmp_path / "DSeqPwCNS_10"
    plate.mkdir()
    image = plate / "DSeqPwCNS_10_A1_1.tif"
    image.write_bytes(b"fake-tiff-placeholder")
    manifest = tmp_path / "manifest.csv"

    write_incucyte_manifest(tmp_path, manifest)

    text = manifest.read_text()
    assert text.splitlines()[0] == (
        "plate_name,well,site,timepoint,filename,phase_image_path"
    )
    assert "DSeqPwCNS_10,A1,1,NA,DSeqPwCNS_10_A1_1.tif," in text
    assert str(Path(image)) in text
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```powershell
pytest tests/test_incucyte.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'cptools2.incucyte'`.

- [ ] **Step 3: Implement parser and manifest writer**

Create `cptools2/incucyte.py` with:

```python
"""IncuCyte S3 phase/brightfield TIFF discovery helpers."""

import csv
import re
from pathlib import Path


class IncuCyteParseError(ValueError):
    """Raised when an IncuCyte TIFF filename cannot be parsed."""


_TIMEPOINT_RE = r"\d+d\d+h\d+m"
_PATTERN = re.compile(
    rf"^(?P<plate>.+)_(?P<well>[A-Za-z]+\d+)_(?P<site>\d+)"
    rf"(?:_(?P<timepoint>{_TIMEPOINT_RE}))?\.tif$",
    re.IGNORECASE,
)

MANIFEST_COLUMNS = [
    "plate_name",
    "well",
    "site",
    "timepoint",
    "filename",
    "phase_image_path",
]


def parse_incucyte_filename(filename):
    """Parse an IncuCyte S3 TIFF filename into traceability metadata."""
    name = Path(filename).name
    match = _PATTERN.match(name)
    if not match:
        raise IncuCyteParseError(f"Cannot parse IncuCyte TIFF filename: {name}")
    return {
        "plate_name": match.group("plate"),
        "well": match.group("well").upper(),
        "site": match.group("site"),
        "timepoint": match.group("timepoint"),
        "filename": name,
    }


def discover_incucyte_images(root):
    """Discover IncuCyte TIFFs below a parent directory and return manifest rows."""
    rows = []
    for path in sorted(Path(root).glob("*/*.tif")):
        parsed = parse_incucyte_filename(path.name)
        parsed["phase_image_path"] = str(path)
        rows.append(parsed)
    if not rows:
        raise IncuCyteParseError(f"No IncuCyte TIFF files found under: {root}")
    return rows


def write_incucyte_manifest(root, output_path):
    """Write a CSV manifest with a canonical phase_image_path column."""
    rows = discover_incucyte_images(root)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        for row in rows:
            record = dict(row)
            if record["timepoint"] is None:
                record["timepoint"] = "NA"
            writer.writerow(record)
    return output_path
```

- [ ] **Step 4: Run parser tests**

Run:

```powershell
pytest tests/test_incucyte.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

```powershell
git add cptools2/incucyte.py tests/test_incucyte.py
git commit -m "feat: add IncuCyte phase image manifest parser"
```

---

### Task 2: DINOv2 Config and Container Role Contract

**Files:**
- Modify: `cptools2/parse_yaml.py`
- Modify: `cptools2/containers.py`
- Modify: `tests/test_parse_yaml.py`
- Modify: `tests/test_containers.py`

- [ ] **Step 1: Add config tests**

Append to `tests/test_parse_yaml.py`:

```python
def test_dinov2_phase_extraction_allows_segment_free_config():
    yaml_dict = {
        "experiment": "/tmp/incucyte",
        "location": "/tmp/output",
        "feature_extraction": {
            "tool": "dinov2",
            "modality": "phase",
            "model": "dinov2_vitl14",
            "weights": "${CPTOOLS2_MODEL_DIR}/dinov2/dinov2_vitl14.pth",
            "image_channel": "phase_image_path",
            "batch_size": 32,
            "output_format": "parquet",
        },
    }

    parse_yaml.validate_ai_segmentation_contract(yaml_dict)


def test_dinov2_fluorescence_still_requires_cellpose_when_not_phase():
    yaml_dict = {
        "experiment": "/tmp/input",
        "location": "/tmp/output",
        "feature_extraction": {
            "tool": "dinov2",
            "modality": "fluorescence",
        },
        "segmentation": {"engine": "cellprofiler"},
    }

    with pytest.raises(ValueError, match="requires Cellpose"):
        parse_yaml.validate_ai_segmentation_contract(yaml_dict)


def test_generate_params_json_dinov2_phase_fields(tmp_path):
    config = {
        "experiment_args": {"exp_dir": "/tmp/incucyte"},
        "create_command_args": {"location": str(tmp_path)},
        "chunk_args": {"job_size": 8},
        "stage_data": False,
        "feature_extraction": {
            "tool": "dinov2",
            "modality": "phase",
            "model": "dinov2_vitl14",
            "weights": "${CPTOOLS2_MODEL_DIR}/dinov2/dinov2_vitl14.pth",
            "image_channel": "phase_image_path",
            "batch_size": 32,
            "output_format": "parquet",
        },
    }
    params_path = tmp_path / "params.json"

    parse_yaml.generate_params_json(config, params_path)

    with params_path.open() as handle:
        params = json.load(handle)
    assert params["feature_extraction_tool"] == "dinov2"
    assert params["feature_extraction_modality"] == "phase"
    assert params["feature_extraction_model"] == "dinov2_vitl14"
    assert params["feature_extraction_image_channel"] == "phase_image_path"
    assert params["feature_extraction_output_format"] == "parquet"
    assert params["feature_extraction_weights"] == (
        "${CPTOOLS2_MODEL_DIR}/dinov2/dinov2_vitl14.pth"
    )
```

Append to `tests/test_containers.py`:

```python
def test_default_containers_include_dinov2():
    from cptools2.containers import DEFAULT_CONTAINERS

    assert DEFAULT_CONTAINERS["dinov2"] == "dinov2_1.0.sif"
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```powershell
pytest tests/test_parse_yaml.py::test_dinov2_phase_extraction_allows_segment_free_config tests/test_parse_yaml.py::test_generate_params_json_dinov2_phase_fields tests/test_containers.py::test_default_containers_include_dinov2 -q
```

Expected: FAIL because DINOv2 params and role are not implemented yet.

- [ ] **Step 3: Update container defaults**

In `cptools2/containers.py`, update `DEFAULT_CONTAINERS`:

```python
DEFAULT_CONTAINERS = {
    "cellprofiler": "cellprofiler_4.2.8.sif",
    "deepprofiler": "deepprofiler_1.0.sif",
    "cellpose": "cellpose_sam_1.0.sif",
    "cellpose_sam": "cellpose_sam_1.0.sif",
    "dinov2": "dinov2_1.0.sif",
}
```

- [ ] **Step 4: Update segmentation validation**

In `cptools2/parse_yaml.py`, replace `validate_ai_segmentation_contract` with:

```python
def validate_ai_segmentation_contract(yaml_dict):
    """AI feature extraction paths require Cellpose unless explicitly segment-free."""
    feature_extraction = yaml_dict.get("feature_extraction")
    if not isinstance(feature_extraction, dict):
        return
    tool = str(feature_extraction.get("tool", "")).lower()
    if tool in {"", "cellprofiler"}:
        return
    modality = str(feature_extraction.get("modality", "")).lower()
    segment_free_dinov2 = tool == "dinov2" and modality in {"phase", "brightfield"}
    if segment_free_dinov2:
        return
    segmentation = yaml_dict.get("segmentation", {})
    engine = ""
    if isinstance(segmentation, dict):
        engine = str(segmentation.get("engine", "")).lower()
    elif isinstance(segmentation, str):
        engine = segmentation.lower()
    if engine and engine != "cellpose":
        raise ValueError(
            "AI feature extraction tool '{}' requires Cellpose segmentation. "
            "Set segmentation.engine: cellpose or omit segmentation.engine to use "
            "the AI pipeline default.".format(tool)
        )
```

- [ ] **Step 5: Emit DINOv2 params**

In `generate_params_json`, inside the existing `feature_extraction` block, add:

```python
            if "model" in feature_extraction:
                params["feature_extraction_model"] = feature_extraction["model"]
            if "modality" in feature_extraction:
                params["feature_extraction_modality"] = feature_extraction["modality"]
            if "image_channel" in feature_extraction:
                params["feature_extraction_image_channel"] = feature_extraction[
                    "image_channel"
                ]
            if "output_format" in feature_extraction:
                params["feature_extraction_output_format"] = feature_extraction[
                    "output_format"
                ]
```

In `parse_config_file`, update the resolved role list:

```python
        for role in ["cellprofiler", "deepprofiler", "cellpose_sam", "dinov2"]:
```

- [ ] **Step 6: Run config and container tests**

Run:

```powershell
pytest tests/test_parse_yaml.py tests/test_containers.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit Task 2**

```powershell
git add cptools2/parse_yaml.py cptools2/containers.py tests/test_parse_yaml.py tests/test_containers.py
git commit -m "feat: add DINOv2 phase extraction config contract"
```

---

### Task 3: Local DINO Embedding CLI With Stub Adapter

**Files:**
- Modify: `pyproject.toml`
- Create: `cptools2/dino_embed.py`
- Create: `tests/test_dino_embed.py`

- [ ] **Step 1: Add extractor dependencies**

In `pyproject.toml`, add these dependencies to `[project].dependencies`:

```toml
    "numpy>=1.24",
    "pillow>=10.0",
    "pyarrow>=14.0",
```

Do not add PyTorch to base package dependencies. The DINOv2 container owns PyTorch.

- [ ] **Step 2: Write local extractor tests**

Create `tests/test_dino_embed.py` with:

```python
import csv
import json

import numpy as np
import pyarrow.parquet as pq
from PIL import Image

from cptools2.dino_embed import ConstantAdapter, run_embedding


def _write_image(path):
    data = np.arange(64, dtype=np.uint8).reshape(8, 8)
    Image.fromarray(data).save(path)


def _write_manifest(path, image_path):
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "plate_name",
                "well",
                "site",
                "timepoint",
                "filename",
                "phase_image_path",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "plate_name": "DSeqPwCNS_1",
                "well": "A1",
                "site": "1",
                "timepoint": "00d00h00m",
                "filename": image_path.name,
                "phase_image_path": str(image_path),
            }
        )


def test_run_embedding_writes_parquet_and_metadata(tmp_path):
    image_path = tmp_path / "DSeqPwCNS_1_A1_1_00d00h00m.tif"
    manifest_path = tmp_path / "manifest.csv"
    output_path = tmp_path / "features" / "dinov2_embeddings.parquet"
    metadata_path = tmp_path / "features" / "dinov2_metadata.json"
    _write_image(image_path)
    _write_manifest(manifest_path, image_path)

    run_embedding(
        chunk_manifest=manifest_path,
        output=output_path,
        metadata_output=metadata_path,
        adapter=ConstantAdapter(embedding_dim=4),
        model_family="dinov2",
        model_name="dinov2_vitl14",
        weights="stub-weights",
        modality="phase",
        image_column="phase_image_path",
        batch_size=2,
    )

    table = pq.read_table(output_path)
    data = table.to_pylist()
    assert len(data) == 1
    assert data[0]["plate_name"] == "DSeqPwCNS_1"
    assert data[0]["well"] == "A1"
    assert data[0]["site"] == "1"
    assert data[0]["timepoint"] == "00d00h00m"
    assert data[0]["model_name"] == "dinov2_vitl14"
    assert data[0]["model_family"] == "dinov2"
    assert data[0]["modality"] == "phase"
    assert data[0]["embedding"] == [1.0, 1.0, 1.0, 1.0]

    metadata = json.loads(metadata_path.read_text())
    assert metadata["row_count"] == 1
    assert metadata["embedding_dim"] == 4
    assert metadata["image_column"] == "phase_image_path"
    assert metadata["preprocessing"]["phase_to_rgb"] is True


def test_run_embedding_fails_on_missing_image_column(tmp_path):
    manifest_path = tmp_path / "manifest.csv"
    output_path = tmp_path / "out.parquet"
    metadata_path = tmp_path / "metadata.json"
    with manifest_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["plate_name"])
        writer.writeheader()
        writer.writerow({"plate_name": "DSeqPwCNS_1"})

    try:
        run_embedding(
            chunk_manifest=manifest_path,
            output=output_path,
            metadata_output=metadata_path,
            adapter=ConstantAdapter(embedding_dim=4),
            model_family="dinov2",
            model_name="dinov2_vitl14",
            weights="stub-weights",
            modality="phase",
            image_column="phase_image_path",
            batch_size=2,
        )
    except ValueError as exc:
        assert "Missing image column" in str(exc)
    else:
        raise AssertionError("run_embedding should fail on a missing image column")
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```powershell
pytest tests/test_dino_embed.py -q
```

Expected: FAIL because `cptools2.dino_embed` does not exist.

- [ ] **Step 4: Implement local extractor**

Create `cptools2/dino_embed.py` with:

```python
"""DINO-family whole-image embedding extraction for cptools2."""

import argparse
import csv
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image


class ConstantAdapter:
    """Small adapter used by tests to avoid real model weights."""

    family = "stub"

    def __init__(self, embedding_dim=4):
        self.embedding_dim = int(embedding_dim)

    def embed(self, images):
        return [
            np.ones(self.embedding_dim, dtype=np.float32)
            for _ in images
        ]


def _read_manifest(path):
    with Path(path).open(newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        rows = list(reader)
    return fieldnames, rows


def _load_phase_image(path):
    image = Image.open(path)
    array = np.asarray(image)
    if array.ndim == 2:
        array = np.stack([array, array, array], axis=-1)
    elif array.ndim == 3 and array.shape[-1] == 1:
        array = np.repeat(array, 3, axis=-1)
    elif array.ndim == 3 and array.shape[-1] >= 3:
        array = array[..., :3]
    else:
        raise ValueError(f"Unsupported image shape for DINOv2: {array.shape}")
    return array


def _normalise_timepoint(value):
    if value in {None, "", "NA"}:
        return None
    return value


def _write_parquet(rows, output):
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, output)


def run_embedding(
    chunk_manifest,
    output,
    metadata_output,
    adapter,
    model_family,
    model_name,
    weights,
    modality,
    image_column,
    batch_size,
):
    fieldnames, manifest_rows = _read_manifest(chunk_manifest)
    if image_column not in fieldnames:
        raise ValueError(f"Missing image column in manifest: {image_column}")
    if not manifest_rows:
        raise ValueError(f"No rows found in manifest: {chunk_manifest}")

    output_rows = []
    batch_images = []
    batch_rows = []

    def flush_batch():
        if not batch_images:
            return
        embeddings = adapter.embed(batch_images)
        for source, embedding in zip(batch_rows, embeddings):
            values = np.asarray(embedding, dtype=np.float32).tolist()
            output_rows.append(
                {
                    "plate_name": source.get("plate_name", ""),
                    "well": source.get("well", ""),
                    "site": source.get("site", ""),
                    "timepoint": _normalise_timepoint(source.get("timepoint")),
                    "image_path": source[image_column],
                    "model_name": model_name,
                    "model_family": model_family,
                    "modality": modality,
                    "preprocessing": "phase_to_rgb_resize_normalize",
                    "embedding": values,
                }
            )
        batch_images.clear()
        batch_rows.clear()

    for row in manifest_rows:
        image_path = Path(row[image_column])
        if not image_path.is_file():
            raise FileNotFoundError(f"Source image does not exist: {image_path}")
        batch_images.append(_load_phase_image(image_path))
        batch_rows.append(row)
        if len(batch_images) >= int(batch_size):
            flush_batch()
    flush_batch()

    if not output_rows:
        raise ValueError("DINO embedding output would be empty")
    embedding_dim = len(output_rows[0]["embedding"])
    for row in output_rows:
        if len(row["embedding"]) != embedding_dim:
            raise ValueError("DINO embedding dimension mismatch")

    _write_parquet(output_rows, output)
    metadata = {
        "model_family": model_family,
        "model_name": model_name,
        "weights": str(weights),
        "modality": modality,
        "image_column": image_column,
        "row_count": len(output_rows),
        "source_image_count": len(manifest_rows),
        "embedding_dim": embedding_dim,
        "preprocessing": {
            "phase_to_rgb": True,
            "strategy": "whole_image",
        },
    }
    metadata_output = Path(metadata_output)
    metadata_output.parent.mkdir(parents=True, exist_ok=True)
    metadata_output.write_text(json.dumps(metadata, indent=2) + "\n")
    return output


def build_parser():
    parser = argparse.ArgumentParser(
        description="Extract whole-image DINO-family embeddings."
    )
    parser.add_argument("--chunk-manifest", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--metadata-output", required=True)
    parser.add_argument("--model-family", default="dinov2")
    parser.add_argument("--model-name", default="dinov2_vitl14")
    parser.add_argument("--weights", required=True)
    parser.add_argument("--modality", default="phase")
    parser.add_argument("--image-column", default="phase_image_path")
    parser.add_argument("--batch-size", type=int, default=32)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.model_family != "dinov2":
        raise ValueError(f"Unsupported model family: {args.model_family}")
    from cptools2.dinov2_model import DinoV2Adapter

    adapter = DinoV2Adapter(args.model_name, args.weights)
    return run_embedding(
        chunk_manifest=args.chunk_manifest,
        output=args.output,
        metadata_output=args.metadata_output,
        adapter=adapter,
        model_family=args.model_family,
        model_name=args.model_name,
        weights=args.weights,
        modality=args.modality,
        image_column=args.image_column,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run local extractor tests**

Run:

```powershell
pytest tests/test_dino_embed.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit Task 3**

```powershell
git add pyproject.toml cptools2/dino_embed.py tests/test_dino_embed.py
git commit -m "feat: add local DINO embedding extractor contract"
```

---

### Task 4: Real DINOv2 Adapter and Optional Model-Load Test

**Files:**
- Create: `cptools2/dinov2_model.py`
- Modify: `tests/test_dino_embed.py`

- [ ] **Step 1: Add optional real-weight test**

Append to `tests/test_dino_embed.py`:

```python
import os

import pytest


@pytest.mark.skipif(
    not os.environ.get("CPTOOLS2_DINOV2_WEIGHTS"),
    reason="CPTOOLS2_DINOV2_WEIGHTS is not set",
)
def test_dinov2_adapter_loads_configured_weights():
    from cptools2.dinov2_model import DinoV2Adapter

    adapter = DinoV2Adapter(
        model_name=os.environ.get("CPTOOLS2_DINOV2_MODEL", "dinov2_vitl14"),
        weights=os.environ["CPTOOLS2_DINOV2_WEIGHTS"],
        repo_path=os.environ.get("CPTOOLS2_DINOV2_REPO"),
        device="cpu",
    )

    assert adapter.embedding_dim == 1024
```

- [ ] **Step 2: Run optional test and verify skip**

Run without weights:

```powershell
pytest tests/test_dino_embed.py::test_dinov2_adapter_loads_configured_weights -q
```

Expected: SKIPPED.

- [ ] **Step 3: Implement DINOv2 adapter**

Create `cptools2/dinov2_model.py` with:

```python
"""DINOv2 model adapter used by cptools2.dino_embed."""

from pathlib import Path

import numpy as np


class DinoV2Adapter:
    """Load a DINOv2 model from a local repository and mounted checkpoint."""

    family = "dinov2"

    def __init__(self, model_name, weights, repo_path=None, device=None):
        self.model_name = model_name
        self.weights = Path(weights)
        if not self.weights.is_file():
            raise FileNotFoundError(f"DINOv2 weights do not exist: {self.weights}")
        self.repo_path = repo_path
        self.device = device
        self.model = self._load_model()
        self.embedding_dim = self._embedding_dim_for_model(model_name)

    @staticmethod
    def _embedding_dim_for_model(model_name):
        dims = {
            "dinov2_vits14": 384,
            "dinov2_vitb14": 768,
            "dinov2_vitl14": 1024,
            "dinov2_vitg14": 1536,
        }
        if model_name not in dims:
            raise ValueError(f"Unsupported DINOv2 model name: {model_name}")
        return dims[model_name]

    def _load_model(self):
        import torch

        device = self.device or ("cuda" if torch.cuda.is_available() else "cpu")
        if self.repo_path:
            model = torch.hub.load(
                self.repo_path,
                self.model_name,
                source="local",
                pretrained=False,
            )
        else:
            model = torch.hub.load(
                "facebookresearch/dinov2",
                self.model_name,
                pretrained=False,
            )
        state = torch.load(self.weights, map_location="cpu")
        if isinstance(state, dict) and "model" in state:
            state = state["model"]
        model.load_state_dict(state, strict=False)
        model.eval()
        model.to(device)
        self.device = device
        return model

    def _prepare_tensor(self, image):
        import torch
        import torch.nn.functional as F

        array = np.asarray(image, dtype=np.float32)
        if array.max() > 1.0:
            array = array / 255.0
        tensor = torch.from_numpy(array).permute(2, 0, 1).unsqueeze(0)
        tensor = F.interpolate(
            tensor,
            size=(518, 518),
            mode="bilinear",
            align_corners=False,
        )
        mean = torch.tensor([0.485, 0.456, 0.406]).view(1, 3, 1, 1)
        std = torch.tensor([0.229, 0.224, 0.225]).view(1, 3, 1, 1)
        tensor = (tensor - mean) / std
        return tensor.to(self.device)

    def embed(self, images):
        import torch

        outputs = []
        with torch.no_grad():
            for image in images:
                tensor = self._prepare_tensor(image)
                features = self.model(tensor)
                if isinstance(features, dict):
                    features = features.get("x_norm_clstoken") or next(
                        iter(features.values())
                    )
                vector = features.detach().cpu().numpy().reshape(-1)
                outputs.append(vector.astype(np.float32))
        return outputs
```

- [ ] **Step 4: Run local tests**

Run:

```powershell
pytest tests/test_dino_embed.py -q
```

Expected: PASS with the real-weight test skipped unless `CPTOOLS2_DINOV2_WEIGHTS` is set.

- [ ] **Step 5: Commit Task 4**

```powershell
git add cptools2/dinov2_model.py tests/test_dino_embed.py
git commit -m "feat: add DINOv2 model adapter"
```

---

### Task 5: Container Manifest and Docker Build

**Files:**
- Create: `cptools2/dockerfiles/Dockerfile.dinov2`
- Modify: `cptools2/container_manifest_template.json`
- Modify: `cptools2/dockerfiles/build_containers.sh`
- Modify: `cptools2/dockerfiles/README.md`
- Modify: `tests/test_eddie_runtime_config.py`

- [ ] **Step 1: Add manifest/build tests**

Append to `tests/test_eddie_runtime_config.py`:

```python
def test_dinov2_container_manifest_and_build_script_are_documented():
    manifest = (ROOT / "cptools2" / "container_manifest_template.json").read_text()
    build = (ROOT / "cptools2" / "dockerfiles" / "build_containers.sh").read_text()
    readme = (ROOT / "cptools2" / "dockerfiles" / "README.md").read_text()

    assert '"dinov2"' in manifest
    assert '"image": "dinov2_1.0.sif"' in manifest
    assert '"archive": "dinov2_1.0.tar"' in manifest
    assert '"recipe": "Dockerfile.dinov2"' in manifest
    assert "dinov2_1.0.sif" in build
    assert "dinov2_1.0.tar" in build
    assert "Dockerfile.dinov2" in readme
    assert "cptools2/dinov2:1.0" in readme
```

- [ ] **Step 2: Run test and verify failure**

Run:

```powershell
pytest tests/test_eddie_runtime_config.py::test_dinov2_container_manifest_and_build_script_are_documented -q
```

Expected: FAIL because DINOv2 container metadata is not present.

- [ ] **Step 3: Create Dockerfile**

Create `cptools2/dockerfiles/Dockerfile.dinov2`:

```dockerfile
FROM pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV CPTOOLS2_DINOV2_REPO=/opt/dinov2

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 https://github.com/facebookresearch/dinov2.git /opt/dinov2

RUN pip install --no-cache-dir \
    numpy>=1.24 \
    pillow>=10.0 \
    pyarrow>=14.0 \
    pyyaml>=5.4 \
    polars>=1.0

WORKDIR /opt/cptools2
COPY . /opt/cptools2
RUN pip install --no-cache-dir --no-deps -e .

CMD ["python", "-m", "cptools2.dino_embed", "--help"]
```

- [ ] **Step 4: Update manifest**

In `cptools2/container_manifest_template.json`, add this sibling entry under `"containers"`:

```json
        "dinov2": {
            "image": "dinov2_1.0.sif",
            "archive": "dinov2_1.0.tar",
            "version": "1.0",
            "docker_image": "cptools2/dinov2:1.0",
            "recipe": "Dockerfile.dinov2",
            "gpu": true,
            "sha256": null,
            "verified": false
        }
```

- [ ] **Step 5: Update build script**

In `cptools2/dockerfiles/build_containers.sh`, after DeepProfiler build, add:

```bash
echo "=== Building DINOv2 ==="
if [[ -s "$CONTAINER_DIR/dinov2_1.0.sif" ]]; then
  echo "DINOv2 SIF already present; skipping"
else
  singularity build --force "$CONTAINER_DIR/dinov2_1.0.sif" \
    oci-archive://"$CONTAINER_DIR/dinov2_1.0.tar"
fi
```

Also update archive validation text in the script so it lists `dinov2_1.0.tar`.

- [ ] **Step 6: Update Dockerfile README**

Add DINOv2 to `cptools2/dockerfiles/README.md` build commands:

```bash
docker build -f Dockerfile.dinov2 -t cptools2/dinov2:1.0 ..
docker run --gpus all --rm cptools2/dinov2:1.0 python -m cptools2.dino_embed --help
docker save -o dinov2_1.0.tar cptools2/dinov2:1.0
```

Add `dinov2_1.0.sif` to the expected Eddie container list.

- [ ] **Step 7: Run container metadata test**

Run:

```powershell
pytest tests/test_eddie_runtime_config.py::test_dinov2_container_manifest_and_build_script_are_documented -q
```

Expected: PASS.

- [ ] **Step 8: Commit Task 5**

```powershell
git add cptools2/dockerfiles/Dockerfile.dinov2 cptools2/container_manifest_template.json cptools2/dockerfiles/build_containers.sh cptools2/dockerfiles/README.md tests/test_eddie_runtime_config.py
git commit -m "feat: add DINOv2 container build contract"
```

---

### Task 6: Nextflow DINOv2 Integration

**Files:**
- Modify: `nextflow/modules/feature_extract.nf`
- Modify: `nextflow/main.nf`
- Modify: `nextflow/conf/containers.config`
- Modify: `nextflow/conf/eddie.config`
- Modify: `tests/test_nextflow_architecture_smoke.py`

- [ ] **Step 1: Add architecture tests**

Append to `tests/test_nextflow_architecture_smoke.py`:

```python
def test_nextflow_feature_extract_dinov2_uses_cptools2_embedder():
    feature_extract = (
        ROOT / "nextflow" / "modules" / "feature_extract.nf"
    ).read_text()

    assert "case 'dinov2':" in feature_extract
    assert "params.containers.dinov2" in feature_extract
    assert "python -m cptools2.dino_embed" in feature_extract
    assert "--image-column ${params.feature_extraction_image_column}" in feature_extract
    assert "--model-name ${params.feature_extraction_model}" in feature_extract
    assert "dinov2_embeddings.parquet" in feature_extract
    assert "dinov2_metadata.json" in feature_extract
    assert "python -m extract_features" not in feature_extract


def test_nextflow_eddie_config_uses_dinov2_sif():
    eddie = (ROOT / "nextflow" / "conf" / "eddie.config").read_text()

    assert 'dinov2: "${cptools2ContainerDir}/dinov2_1.0.sif"' in eddie
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```powershell
pytest tests/test_nextflow_architecture_smoke.py::test_nextflow_feature_extract_dinov2_uses_cptools2_embedder tests/test_nextflow_architecture_smoke.py::test_nextflow_eddie_config_uses_dinov2_sif -q
```

Expected: FAIL because DINOv2 still uses placeholder command and Docker image in Eddie config.

- [ ] **Step 3: Update Nextflow defaults**

In `nextflow/main.nf`, add defaults near feature extraction params:

```groovy
params.feature_extraction_model       = 'dinov2_vitl14'
params.feature_extraction_modality    = 'phase'
params.feature_extraction_image_column = 'phase_image_path'
params.feature_extraction_output_format = 'parquet'
```

- [ ] **Step 4: Update feature_extract DINOv2 branch**

In `nextflow/modules/feature_extract.nf`, replace the DINOv2 placeholder script with:

```groovy
    } else if (params.feature_extraction_tool == 'dinov2') {
        def weights_path = params.feature_extraction_weights && params.feature_extraction_weights.toString() != 'null'
            ? params.feature_extraction_weights.toString()
            : ''
        """
        mkdir -p features

        echo '=== cptools2 GPU diagnostics: FEATURE_EXTRACT start ==='
        hostname || true
        echo "CUDA_VISIBLE_DEVICES=\${CUDA_VISIBLE_DEVICES:-unset}"
        nvidia-smi -L || true
        nvidia-smi --query-gpu=index,name,memory.total,memory.used --format=csv || true
        echo '=== cptools2 GPU diagnostics: FEATURE_EXTRACT end ==='

        if [ -z "${weights_path}" ]; then
            echo "DINOv2 weights must be configured with feature_extraction.weights" >&2
            exit 1
        fi
        if [ ! -f "${weights_path}" ]; then
            echo "Configured DINOv2 weights do not exist: ${weights_path}" >&2
            exit 1
        fi
        if [ -n "\${CPTOOLS2_PROJECT_ROOT:-}" ]; then
            export PYTHONPATH="\${CPTOOLS2_PROJECT_ROOT}:\${PYTHONPATH:-}"
        fi

        python -m cptools2.dino_embed \\
            --chunk-manifest ${chunk_manifest} \\
            --output features/dinov2_embeddings.parquet \\
            --metadata-output features/dinov2_metadata.json \\
            --model-family dinov2 \\
            --model-name ${params.feature_extraction_model} \\
            --weights ${weights_path} \\
            --modality ${params.feature_extraction_modality} \\
            --image-column ${params.feature_extraction_image_column} \\
            --batch-size ${params.feature_extraction_batch_size}

        test -s features/dinov2_embeddings.parquet
        test -s features/dinov2_metadata.json

        echo '=== cptools2 GPU diagnostics: FEATURE_EXTRACT post ==='
        nvidia-smi --query-gpu=index,name,memory.total,memory.used --format=csv || true
        echo '=== cptools2 GPU diagnostics: FEATURE_EXTRACT post end ==='
        """
    } else
        error "Unknown feature extraction tool: ${params.feature_extraction_tool}. Supported: deepprofiler, dinov2"
```

- [ ] **Step 5: Update Eddie container mapping**

In `nextflow/conf/eddie.config`, change:

```groovy
        dinov2: 'cptools2/dinov2:1.0'
```

to:

```groovy
        dinov2: "${cptools2ContainerDir}/dinov2_1.0.sif"
```

- [ ] **Step 6: Run architecture tests**

Run:

```powershell
pytest tests/test_nextflow_architecture_smoke.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit Task 6**

```powershell
git add nextflow/modules/feature_extract.nf nextflow/main.nf nextflow/conf/containers.config nextflow/conf/eddie.config tests/test_nextflow_architecture_smoke.py
git commit -m "feat: wire DINOv2 extractor into Nextflow"
```

---

### Task 7: Branch-Scoped Smoke Config and User Docs

**Files:**
- Create: `config/dinov2-phase-smoke.yaml`
- Modify: `docs/reference/nextflow-config-yaml.md`
- Modify: `.claude/plans/phase-2.9-dinov2-phase-feature-extraction.md`

- [ ] **Step 1: Create branch-scoped smoke config**

Create `config/dinov2-phase-smoke.yaml`:

```yaml
# Branch-scoped smoke config for ai-update-DINO.
# This config is not part of the default Phase 2.8 queue.

input_dir: ${CPTOOLS2_SCRATCH_ROOT}/staging/dinov2-incucyte-smoke/tiff_files
output_dir: ${CPTOOLS2_SCRATCH_ROOT}/results/dinov2-phase-smoke
commands location: ${CPTOOLS2_SCRATCH_ROOT}/results/dinov2-phase-smoke/commands
container_path: ${CPTOOLS2_CONTAINER_DIR}
stage_data: false
stages:
  - extract
chunk: 8
max_chunks: 1

feature_extraction:
  tool: dinov2
  modality: phase
  model: dinov2_vitl14
  weights: ${CPTOOLS2_MODEL_DIR}/dinov2/dinov2_vitl14.pth
  image_channel: phase_image_path
  batch_size: 32
  output_format: parquet
```

- [ ] **Step 2: Document config keys**

In `docs/reference/nextflow-config-yaml.md`, extend `feature_extraction` with:

```markdown
For branch-scoped DINOv2 phase/brightfield extraction:

```yaml
feature_extraction:
  tool: dinov2
  modality: phase
  model: dinov2_vitl14
  weights: ${CPTOOLS2_MODEL_DIR}/dinov2/dinov2_vitl14.pth
  image_channel: phase_image_path
  batch_size: 32
  output_format: parquet
```

`dinov2` currently means whole-image/site embeddings for IncuCyte-style
phase/brightfield TIFFs. The first manifest convention is `phase_image_path`.
IncuCyte outputs always include a `timepoint` column; files without an elapsed
time token emit `NA`/null.

The DINOv2 model weights are site assets under `CPTOOLS2_MODEL_DIR`, not Git
artifacts and not runtime downloads.
```

- [ ] **Step 3: Link implementation plan from phase plan**

In `.claude/plans/phase-2.9-dinov2-phase-feature-extraction.md`, add under `source_context`:

```yaml
implementation_plan:
  - docs/superpowers/plans/2026-05-14-dinov2-phase-feature-extraction-implementation.md
```

- [ ] **Step 4: Run docs/config parse smoke**

Run:

```powershell
pytest tests/test_parse_yaml.py::test_generate_params_json_dinov2_phase_fields -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 7**

```powershell
git add config/dinov2-phase-smoke.yaml docs/reference/nextflow-config-yaml.md .claude/plans/phase-2.9-dinov2-phase-feature-extraction.md
git commit -m "docs: add DINOv2 phase smoke config"
```

---

### Task 8: Eddie DINOv2 Model-Load Smoke Script

**Files:**
- Create: `scripts/eddie_smoke_dinov2.sh`
- Modify: `tests/test_eddie_runtime_config.py`

- [ ] **Step 1: Add smoke-script test**

Append to `tests/test_eddie_runtime_config.py`:

```python
def test_dinov2_smoke_script_uses_gpu_and_mounted_weights():
    script = (ROOT / "scripts" / "eddie_smoke_dinov2.sh").read_text()

    assert "#$ -q gpu" in script
    assert "-l gpu=1" in script
    assert "module load singularity/4.3.4" in script
    assert "dinov2_1.0.sif" in script
    assert "singularity exec --nv" in script
    assert "${CPTOOLS2_MODEL_DIR}/dinov2/dinov2_vitl14.pth" in script
    assert "python -m cptools2.dino_embed --help" in script
    assert "CPTOOLS2_DINOV2_REPO=/opt/dinov2" in script
```

- [ ] **Step 2: Run test and verify failure**

Run:

```powershell
pytest tests/test_eddie_runtime_config.py::test_dinov2_smoke_script_uses_gpu_and_mounted_weights -q
```

Expected: FAIL because the smoke script does not exist.

- [ ] **Step 3: Create Eddie smoke script**

Create `scripts/eddie_smoke_dinov2.sh`:

```bash
#!/bin/bash
#$ -N cptools2_dinov2_smoke
#$ -q gpu
#$ -l gpu=1
#$ -l h_rss=16G
#$ -l h_rt=01:00:00
#$ -cwd
#$ -o /exports/eddie/scratch/$USER/cptools2-ai-update/logs/dinov2_$JOB_ID.log
#$ -e /exports/eddie/scratch/$USER/cptools2-ai-update/logs/dinov2_$JOB_ID.err

set -euo pipefail

. /etc/profile.d/modules.sh
module purge
module load singularity/4.3.4

PROJECT_ROOT="${CPTOOLS2_PROJECT_ROOT:?Set CPTOOLS2_PROJECT_ROOT}"
SCRATCH_ROOT="${CPTOOLS2_SCRATCH_ROOT:-/exports/eddie/scratch/$USER/cptools2-ai-update}"
CONTAINER="${CPTOOLS2_CONTAINER_DIR:-${PROJECT_ROOT}/containers}/dinov2_1.0.sif"
WEIGHTS="${CPTOOLS2_MODEL_DIR:-${PROJECT_ROOT}-local/models}/dinov2/dinov2_vitl14.pth"
SMOKE_ROOT="${SCRATCH_ROOT}/smoke/dinov2"

mkdir -p "${SMOKE_ROOT}" "${SCRATCH_ROOT}/logs" \
  "${SCRATCH_ROOT}/work/singularity_tmp" \
  "${SCRATCH_ROOT}/work/singularity_cache"

export SINGULARITY_TMPDIR="${SCRATCH_ROOT}/work/singularity_tmp"
export SINGULARITY_CACHEDIR="${SCRATCH_ROOT}/work/singularity_cache"

test -s "${CONTAINER}"
test -s "${WEIGHTS}"

singularity exec --nv \
  --bind "${PROJECT_ROOT}:${PROJECT_ROOT}" \
  --bind "${CPTOOLS2_MODEL_DIR}:${CPTOOLS2_MODEL_DIR}" \
  "${CONTAINER}" \
  bash -lc '
    set -euo pipefail
    export CPTOOLS2_DINOV2_REPO=/opt/dinov2
    python -m cptools2.dino_embed --help
    python - <<PY
import torch
print("torch", torch.__version__)
print("cuda_available", torch.cuda.is_available())
print("cuda_devices", torch.cuda.device_count())
PY
  '
```

- [ ] **Step 4: Run smoke-script test**

Run:

```powershell
pytest tests/test_eddie_runtime_config.py::test_dinov2_smoke_script_uses_gpu_and_mounted_weights -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 8**

```powershell
git add scripts/eddie_smoke_dinov2.sh tests/test_eddie_runtime_config.py
git commit -m "test: add Eddie DINOv2 model-load smoke script"
```

---

### Task 9: Dry-Run and Local Verification

**Files:**
- No new files unless tests expose a defect.

- [ ] **Step 1: Run focused Python tests**

Run:

```powershell
pytest tests/test_incucyte.py tests/test_dino_embed.py tests/test_parse_yaml.py tests/test_containers.py -q
```

Expected: PASS.

- [ ] **Step 2: Run focused Nextflow/static tests**

Run:

```powershell
pytest tests/test_nextflow_architecture_smoke.py tests/test_eddie_runtime_config.py -q
```

Expected: PASS.

- [ ] **Step 3: Generate params from smoke config**

Run:

```powershell
python -m cptools2.__main__ pipeline config/dinov2-phase-smoke.yaml --dry-run
```

Expected: either PASS when `${CPTOOLS2_SCRATCH_ROOT}` variables resolve locally, or a clear local path/environment error. If local Windows path expansion makes this unsuitable, run the same command on Eddie after sourcing `config/eddie_env.sh`.

- [ ] **Step 4: Commit any verification fixes**

If no code changes were needed, skip this commit. If fixes were needed:

```powershell
git add <changed-files>
git commit -m "fix: stabilize DINOv2 phase extraction verification"
```

---

### Task 10: Eddie Validation Handoff

**Files:**
- Modify only after actual Eddie runs: `.claude/plans/phase-2.9-dinov2-phase-feature-extraction.md`

- [ ] **Step 1: Stage model weights**

On Eddie, after sourcing project environment, verify:

```bash
test -s "${CPTOOLS2_MODEL_DIR}/dinov2/dinov2_vitl14.pth"
sha256sum "${CPTOOLS2_MODEL_DIR}/dinov2/dinov2_vitl14.pth"
```

Expected: checksum is printed. Record it in the Phase 2.9 plan.

- [ ] **Step 2: Build or confirm DINOv2 SIF**

On Eddie:

```bash
test -s "${CPTOOLS2_CONTAINER_DIR}/dinov2_1.0.sif" || qsub cptools2/dockerfiles/build_containers.sh "${CPTOOLS2_CONTAINER_DIR}"
```

Expected: `dinov2_1.0.sif` exists after build completion.

- [ ] **Step 3: Submit model-load smoke**

On Eddie:

```bash
qsub scripts/eddie_smoke_dinov2.sh
```

Expected: SGE job exits 0, CUDA is visible, and `python -m cptools2.dino_embed --help` works inside the container.

- [ ] **Step 4: Stage tiny IncuCyte fixture**

Create this scratch fixture:

```text
${CPTOOLS2_SCRATCH_ROOT}/staging/dinov2-incucyte-smoke/tiff_files/
  DSeqPwCNS_1/
    DSeqPwCNS_1_A1_1_00d00h00m.tif
    DSeqPwCNS_1_A1_1_01d01h16m.tif
    DSeqPwCNS_1_A10_1_00d00h00m.tif
    DSeqPwCNS_1_A10_1_01d01h16m.tif
  DSeqPwCNS_10/
    DSeqPwCNS_10_A1_1.tif
    DSeqPwCNS_10_A2_1.tif
    DSeqPwCNS_10_B1_1.tif
    DSeqPwCNS_10_B2_1.tif
```

Expected: only these eight TIFFs are staged for the first smoke.

- [ ] **Step 5: Run tiny DINOv2 Nextflow smoke**

On Eddie:

```bash
cptools2 pipeline config/dinov2-phase-smoke.yaml
```

Expected:

```text
${CPTOOLS2_SCRATCH_ROOT}/results/dinov2-phase-smoke/
  DSeqPwCNS_*/features/*/dinov2_embeddings.parquet
  DSeqPwCNS_*/features/*/dinov2_metadata.json
  traces/
  params.json
```

- [ ] **Step 6: Record evidence**

Update `.claude/plans/phase-2.9-dinov2-phase-feature-extraction.md` with:

```markdown
## Eddie DINOv2 Evidence

- Weights path:
- Weights sha256:
- Container path:
- Container sha256:
- Smoke SGE job ID:
- GPU resource:
- CUDA visible:
- Embedding rows:
- Embedding dimension:
- Trace path:
- Report path:
- First blocker, if any:
```

- [ ] **Step 7: Run regression tests before promotion**

Run:

```powershell
pytest tests/test_incucyte.py tests/test_dino_embed.py tests/test_parse_yaml.py tests/test_containers.py tests/test_nextflow_architecture_smoke.py tests/test_eddie_runtime_config.py tests/test_nextflow_chunking.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit evidence update**

```powershell
git add .claude/plans/phase-2.9-dinov2-phase-feature-extraction.md
git commit -m "docs: record DINOv2 phase smoke evidence"
```

---

## Self-Review

Spec coverage:

- IncuCyte S3 10X phase/brightfield filenames: Task 1.
- Whole-image TIFF embeddings: Task 3 and Task 6.
- Tiny smoke fixture: Task 10.
- `dinov2_vitl14` checkpoint path: Task 2, Task 7, Task 8, Task 10.
- Nullable `timepoint` output column: Task 1 and Task 3.
- `phase_image_path` manifest convention: Task 1, Task 3, Task 7.
- Segment-free DINOv2 extraction: Task 2 and Task 6.
- Container build and Eddie validation: Task 5, Task 8, Task 10.
- Phase 2.8 conflict control: file structure section and Task 10 regression tests.

Placeholder scan:

- Red-flag scan: clean.

Type consistency:

- Manifest column is consistently `phase_image_path`.
- DINO params are consistently `feature_extraction_model`, `feature_extraction_modality`, `feature_extraction_image_column`, and `feature_extraction_output_format`.
- Extractor module is consistently `cptools2.dino_embed`.
- Real model adapter is consistently `cptools2.dinov2_model.DinoV2Adapter`.
