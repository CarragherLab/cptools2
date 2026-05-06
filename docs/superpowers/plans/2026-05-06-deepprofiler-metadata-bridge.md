# DeepProfiler Metadata Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reusable DeepProfiler input package builder so cptools2 can convert Nextflow chunk manifests and Cellpose centroids into the project layout DeepProfiler expects.

**Architecture:** Add focused Python helpers to `cptools2.nextflow_chunking` for generating a complete DeepProfiler `dp_project/inputs` package: image links, `metadata/index.csv`, per-site nuclei location CSVs, and copied config. Update `nextflow/modules/feature_extract.nf` to call those helpers before running `python -m deepprofiler`, keeping the conversion testable outside Nextflow and reusable for Eddie smoke tests and production runs.

**Tech Stack:** Python stdlib CSV/path handling, existing `cptools2.nextflow_chunking`, Nextflow DSL2 module scripts, pytest, Eddie Singularity/SGE runtime.

---

## Why This Blocker Happens

The Loop 440 smoke now gets through Eddie setup, SGE submission, Cellpose GPU execution, and into DeepProfiler. DeepProfiler fails because `FEATURE_EXTRACT` creates `dp_project/inputs/metadata/chunk_manifest.csv`, but DeepProfiler defaults to reading `dp_project/inputs/metadata/index.csv`.

DeepProfiler also expects metadata to describe one multi-channel field of view per row, with `Metadata_Plate`, `Metadata_Well`, `Metadata_Site`, channel columns such as `DNA`, `RNA`, `ER`, `AGP`, `Mito`, and location CSVs in `inputs/locations/<plate>/<well>-f<site>-Nuclei.csv` containing `Nuclei_Location_Center_X` and `Nuclei_Location_Center_Y`.

The current Cellpose output is close but not conforming:

- `chunk_manifest.csv` is channel-level, not one row per image set.
- Cellpose location output has `x` and `y` columns, not DeepProfiler nuclei column names.
- Location files are linked under `inputs/metadata/locations`, while DeepProfiler expects `inputs/locations`.

## Design Choice

Use option 2 from brainstorming: add a reusable Python helper in `cptools2.nextflow_chunking`.

This is better than embedding a long ad hoc Python heredoc inside Nextflow because the conversion has a real data contract, should be unit tested, and will likely be reused for production DeepProfiler runs.

Important runtime rule: zero Cellpose locations is not an error. A field can contain no cells, especially in a tiny smoke or sparse biological condition. The package builder must still emit a valid DeepProfiler package with `index.csv` and correctly shaped empty nuclei-location CSVs so the pipeline can continue and downstream feature output can represent "no objects" explicitly.

## File Structure

- Modify `cptools2/nextflow_chunking.py`
  - Add `build_deepprofiler_input_package(...)`.
  - Add CLI subcommand `deepprofiler-package`.
  - Keep all CSV conversion logic here.
- Modify `nextflow/modules/feature_extract.nf`
  - Replace ad hoc metadata setup with a call to `python -m cptools2.nextflow_chunking deepprofiler-metadata`.
  - Create `dp_project/inputs/images`, `dp_project/inputs/metadata`, `dp_project/inputs/locations`, config, checkpoint, and output directories.
- Modify `tests/test_nextflow_chunking.py`
  - Add unit tests for DeepProfiler image links, `index.csv` generation, config copying, non-empty location file generation, and zero-location file generation.
- Modify `tests/test_nextflow_architecture_smoke.py`
  - Add static checks that `FEATURE_EXTRACT` uses the helper and the correct DeepProfiler paths.
- Optional docs update after Eddie verification:
  - `.claude/plans/phase-2.8-eddie-container-validation.md`
  - `TODOs.md`

---

### Task 1: Add Failing Tests For DeepProfiler Metadata Conversion

**Files:**
- Modify: `tests/test_nextflow_chunking.py`

- [ ] **Step 1: Add imports used by the new tests**

Add `csv` and `Path` imports at the top of `tests/test_nextflow_chunking.py`:

```python
import csv
import os
from pathlib import Path
```

- [ ] **Step 2: Add a small channel-level manifest fixture in the test file**

Add this helper near the existing constants:

```python
def _write_deepprofiler_manifest(path):
    rows = [
        {
            "plate": "tiny-plate-001",
            "well": "B02",
            "site": "1",
            "channel": "1",
            "image_path": "/scratch/tiny-plate-001/B02_s1_w1.tif",
            "image_set_id": "tiny-plate-001_B02_s1",
            "source_plate_path": "/scratch/tiny-plate-001",
            "chunk_id": "tiny-plate-001_chunk_0001",
            "chunk_number": "1",
        },
        {
            "plate": "tiny-plate-001",
            "well": "B02",
            "site": "1",
            "channel": "2",
            "image_path": "/scratch/tiny-plate-001/B02_s1_w2.tif",
            "image_set_id": "tiny-plate-001_B02_s1",
            "source_plate_path": "/scratch/tiny-plate-001",
            "chunk_id": "tiny-plate-001_chunk_0001",
            "chunk_number": "1",
        },
    ]
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
```

- [ ] **Step 3: Add a location fixture helper**

Add this helper after `_write_deepprofiler_manifest`:

```python
def _write_cellpose_locations(path):
    rows = [
        {
            "plate": "tiny-plate-001",
            "well": "B02",
            "site": "1",
            "image_path": "/scratch/tiny-plate-001/B02_s1_w1.tif",
            "image_set_id": "tiny-plate-001_B02_s1",
            "object_id": "1",
            "x": "12.5",
            "y": "31.25",
            "mask_path": "cellpose_masks/tiny-plate-001_B02_s1_cp_masks.tif",
        }
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
```

- [ ] **Step 4: Add failing test for `index.csv` and location output**

Add this test to `tests/test_nextflow_chunking.py`:

```python
def test_build_deepprofiler_project_metadata_writes_index_and_locations(tmp_path):
    chunk_manifest = tmp_path / "tiny-plate-001_chunk_0001.csv"
    locations_dir = tmp_path / "cellpose_masks" / "locations"
    output_root = tmp_path / "dp_project" / "inputs"

    _write_deepprofiler_manifest(chunk_manifest)
    _write_cellpose_locations(
        locations_dir / "tiny-plate-001_chunk_0001_locations.csv"
    )

    nextflow_chunking.build_deepprofiler_project_metadata(
        chunk_manifest=chunk_manifest,
        locations_dir=locations_dir,
        output_root=output_root,
        channel_map={"1": "DNA", "2": "RNA"},
    )

    index_df = pl.read_csv(output_root / "metadata" / "index.csv")
    assert index_df.columns == [
        "Metadata_Plate",
        "Metadata_Well",
        "Metadata_Site",
        "Metadata_Compound",
        "Metadata_ImageSet",
        "DNA",
        "RNA",
    ]
    assert index_df.to_dicts() == [
        {
            "Metadata_Plate": "tiny-plate-001",
            "Metadata_Well": "B02",
            "Metadata_Site": 1,
            "Metadata_Compound": "DMSO",
            "Metadata_ImageSet": "tiny-plate-001_B02_s1",
            "DNA": "tiny-plate-001/B02_s1_w1.tif",
            "RNA": "tiny-plate-001/B02_s1_w2.tif",
        }
    ]

    nuclei_df = pl.read_csv(
        output_root / "locations" / "tiny-plate-001" / "B02-f1-Nuclei.csv"
    )
    assert nuclei_df.to_dicts() == [
        {
            "Nuclei_Location_Center_X": 12.5,
            "Nuclei_Location_Center_Y": 31.25,
        }
    ]
```

- [ ] **Step 5: Add failing validation test for incomplete channels**

Add this test:

```python
def test_build_deepprofiler_project_metadata_fails_when_channel_missing(tmp_path):
    chunk_manifest = tmp_path / "tiny-plate-001_chunk_0001.csv"
    locations_dir = tmp_path / "cellpose_masks" / "locations"
    output_root = tmp_path / "dp_project" / "inputs"

    _write_deepprofiler_manifest(chunk_manifest)
    _write_cellpose_locations(
        locations_dir / "tiny-plate-001_chunk_0001_locations.csv"
    )

    with pytest.raises(nextflow_chunking.ChunkingError, match="Missing channel"):
        nextflow_chunking.build_deepprofiler_project_metadata(
            chunk_manifest=chunk_manifest,
            locations_dir=locations_dir,
            output_root=output_root,
            channel_map={"1": "DNA", "2": "RNA", "3": "ER"},
        )
```

- [ ] **Step 6: Run tests and verify they fail**

Run:

```bash
pytest tests/test_nextflow_chunking.py::test_build_deepprofiler_project_metadata_writes_index_and_locations tests/test_nextflow_chunking.py::test_build_deepprofiler_project_metadata_fails_when_channel_missing -q
```

Expected: both fail because `build_deepprofiler_project_metadata` does not exist.

- [ ] **Step 7: Add failing zero-location continuation test**

Add this test:

```python
def test_build_deepprofiler_input_package_allows_zero_locations(tmp_path):
    chunk_manifest = tmp_path / "tiny-plate-001_chunk_0001.csv"
    image_root = tmp_path / "staging" / "tiny-plate-001"
    locations_dir = tmp_path / "cellpose_masks" / "locations"
    output_root = tmp_path / "dp_project" / "inputs"
    config_path = tmp_path / "deepprofiler_config.json"

    _write_deepprofiler_manifest(chunk_manifest, image_root)
    _write_deepprofiler_config(config_path)
    locations_dir.mkdir(parents=True)
    (locations_dir / "tiny-plate-001_chunk_0001_locations.csv").write_text(
        "plate,well,site,image_path,image_set_id,object_id,x,y,mask_path\n"
    )

    nextflow_chunking.build_deepprofiler_input_package(
        chunk_manifest=chunk_manifest,
        locations_dir=locations_dir,
        output_root=output_root,
        config_path=config_path,
    )

    nuclei_path = output_root / "locations" / "tiny-plate-001" / "B02-f1-Nuclei.csv"
    assert nuclei_path.exists()
    assert nuclei_path.read_text().strip() == (
        "Nuclei_Location_Center_X,Nuclei_Location_Center_Y"
    )
```

Run:

```bash
pytest tests/test_nextflow_chunking.py::test_build_deepprofiler_input_package_allows_zero_locations -q
```

Expected: fails because `build_deepprofiler_input_package` does not exist.

---

### Task 2: Implement The Reusable DeepProfiler Metadata Helper

**Files:**
- Modify: `cptools2/nextflow_chunking.py`

- [ ] **Step 1: Add helper constants**

Add after `DEFAULT_CHUNK_SIZE = 96`:

```python
DEFAULT_DEEPPROFILER_CHANNEL_MAP = {
    "1": "DNA",
    "2": "RNA",
    "3": "ER",
    "4": "AGP",
    "5": "Mito",
}
```

- [ ] **Step 2: Add channel-map parser**

Add near `_normalise_expected_channels`:

```python
def _parse_channel_map(channel_map=None):
    if channel_map is None:
        return dict(DEFAULT_DEEPPROFILER_CHANNEL_MAP)
    if isinstance(channel_map, dict):
        return {str(key): str(value) for key, value in channel_map.items()}
    parsed = {}
    for item in str(channel_map).split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(
                "channel map entries must use CHANNEL=NAME, got: " + item
            )
        key, value = item.split("=", 1)
        parsed[key.strip()] = value.strip()
    return parsed
```

- [ ] **Step 3: Add image path normalization helper**

Add:

```python
def _deepprofiler_image_value(image_path, plate):
    path = Path(image_path)
    try:
        parts = path.parts
        if plate in parts:
            plate_index = parts.index(plate)
            return str(Path(*parts[plate_index:]))
    except (TypeError, ValueError):
        pass
    return str(Path(plate) / path.name)
```

- [ ] **Step 4: Add metadata builder implementation**

Add before `_cmd_index`:

```python
def build_deepprofiler_input_package(
    chunk_manifest,
    locations_dir,
    output_root,
    config_path,
    channel_map=None,
    label_value=None,
):
    """Write a complete DeepProfiler inputs package for one chunk.

    Parameters
    ----------
    chunk_manifest : str or Path
        Channel-level manifest produced by split_image_set_index.
    locations_dir : str or Path
        Cellpose masks directory or its nested locations directory.
    output_root : str or Path
        DeepProfiler inputs directory, usually dp_project/inputs.
    config_path : str or Path
        DeepProfiler JSON config copied to inputs/config/config.json.
    channel_map : dict or str, optional
        Mapping from manifest channel values to DeepProfiler channel columns.
    label_value : str, optional
        Value for the configured label field. Defaults to the config control value.
    """
    chunk_manifest = Path(chunk_manifest)
    locations_dir = Path(locations_dir)
    output_root = Path(output_root)
    channel_map = _parse_channel_map(channel_map)

    df = pl.read_csv(chunk_manifest).with_columns(pl.col("channel").cast(str))
    required = {"plate", "well", "site", "channel", "image_path", "image_set_id"}
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ChunkingError(
            "DeepProfiler chunk manifest missing required columns: " + str(missing)
        )

    rows = []
    for image_set_id in df["image_set_id"].unique().sort():
        group = df.filter(pl.col("image_set_id") == image_set_id)
        first = group.row(0, named=True)
        row = {
            "Metadata_Plate": first["plate"],
            "Metadata_Well": first["well"],
            "Metadata_Site": first["site"],
            "Metadata_Compound": label_value,
            "Metadata_ImageSet": image_set_id,
        }
        observed = set(group["channel"].to_list())
        for channel, column_name in channel_map.items():
            if channel not in observed:
                raise ChunkingError(
                    f"Missing channel {channel} for DeepProfiler image set {image_set_id}"
                )
            channel_row = group.filter(pl.col("channel") == channel).row(0, named=True)
            row[column_name] = _deepprofiler_image_value(
                channel_row["image_path"], channel_row["plate"]
            )
        rows.append(row)

    metadata_dir = output_root / "metadata"
    locations_output = output_root / "locations"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    locations_output.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_csv(metadata_dir / "index.csv")

    source_locations = locations_dir / "locations"
    if not source_locations.exists():
        source_locations = locations_dir
    location_files = sorted(source_locations.glob("*.csv"))
    if not location_files:
        location_files = []

    for location_file in location_files:
        loc_df = pl.read_csv(location_file)
        required_loc = {"plate", "well", "site", "x", "y"}
        loc_missing = sorted(required_loc.difference(loc_df.columns))
        if loc_missing:
            raise ChunkingError(
                "Cellpose location CSV missing required columns: " + str(loc_missing)
            )
        for key in loc_df.select(["plate", "well", "site"]).unique().iter_rows(named=True):
            plate = str(key["plate"])
            well = str(key["well"])
            site = str(key["site"])
            per_site = loc_df.filter(
                (pl.col("plate").cast(str) == plate)
                & (pl.col("well").cast(str) == well)
                & (pl.col("site").cast(str) == site)
            )
            plate_dir = locations_output / plate
            plate_dir.mkdir(parents=True, exist_ok=True)
            output_file = plate_dir / f"{well}-f{site}-Nuclei.csv"
            per_site.select(
                [
                    pl.col("x").cast(float).alias("Nuclei_Location_Center_X"),
                    pl.col("y").cast(float).alias("Nuclei_Location_Center_Y"),
                ]
            ).write_csv(output_file)

    return metadata_dir / "index.csv"
```

The implementation must write empty per-site nuclei files with headers when Cellpose
produced no rows for an image set. Do not raise an error for zero locations.

- [ ] **Step 5: Add CLI command implementation**

Add:

```python
def _cmd_deepprofiler_package(args):
    build_deepprofiler_input_package(
        chunk_manifest=args.chunk_manifest,
        locations_dir=args.locations_dir,
        output_root=args.output_root,
        config_path=args.config_path,
        channel_map=args.channel_map,
        label_value=args.label_value,
    )
```

- [ ] **Step 6: Register CLI subcommand**

Add before `return parser` in `build_parser()`:

```python
    p_dp = subparsers.add_parser("deepprofiler-package")
    p_dp.add_argument("--chunk-manifest", required=True)
    p_dp.add_argument("--locations-dir", required=True)
    p_dp.add_argument("--output-root", required=True)
    p_dp.add_argument("--config-path", required=True)
    p_dp.add_argument(
        "--channel-map",
        default="1=DNA,2=RNA,3=ER,4=AGP,5=Mito",
        help="Comma-separated mapping from manifest channel id to DeepProfiler column",
    )
    p_dp.add_argument("--label-value", default="DMSO")
    p_dp.set_defaults(func=_cmd_deepprofiler_package)
```

- [ ] **Step 7: Run new unit tests**

Run:

```bash
pytest tests/test_nextflow_chunking.py::test_build_deepprofiler_project_metadata_writes_index_and_locations tests/test_nextflow_chunking.py::test_build_deepprofiler_project_metadata_fails_when_channel_missing -q
```

Expected: both pass.

- [ ] **Step 8: Run full chunking tests**

Run:

```bash
pytest tests/test_nextflow_chunking.py -q
```

Expected: all pass.

- [ ] **Step 9: Commit**

Run:

```bash
git add cptools2/nextflow_chunking.py tests/test_nextflow_chunking.py
git commit -m "add DeepProfiler metadata bridge"
```

---

### Task 3: Wire The Helper Into The Nextflow Feature Extraction Module

**Files:**
- Modify: `nextflow/modules/feature_extract.nf`
- Modify: `tests/test_nextflow_architecture_smoke.py`

- [ ] **Step 1: Add failing architecture test**

Add this test to `tests/test_nextflow_architecture_smoke.py`:

```python
def test_feature_extract_builds_deepprofiler_metadata_contract():
    feature_extract = (
        ROOT / "nextflow" / "modules" / "feature_extract.nf"
    ).read_text()

    assert "deepprofiler-package" in feature_extract
    assert "dp_project/inputs/metadata/index.csv" in feature_extract
    assert "dp_project/inputs/locations" in feature_extract
    assert "inputs/metadata/locations" not in feature_extract
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
pytest tests/test_nextflow_architecture_smoke.py::test_feature_extract_builds_deepprofiler_metadata_contract -q
```

Expected: fails because `FEATURE_EXTRACT` does not call the helper yet.

- [ ] **Step 3: Replace DeepProfiler setup block in `FEATURE_EXTRACT`**

In `nextflow/modules/feature_extract.nf`, replace the DeepProfiler script body with:

```nextflow
        """
        mkdir -p features

        # Set up DeepProfiler project structure
        mkdir -p dp_project/inputs/images/${plate_id}
        mkdir -p dp_project/inputs/metadata
        mkdir -p dp_project/inputs/locations
        mkdir -p dp_project/inputs/config
        mkdir -p dp_project/outputs/cell_painting/checkpoint

        # Link corrected images. DeepProfiler metadata uses paths relative to inputs/images.
        ln -s \$(readlink -f ${corrected_dir})/* dp_project/inputs/images/${plate_id}/

        python -m cptools2.nextflow_chunking deepprofiler-package \\
            --chunk-manifest ${chunk_manifest} \\
            --locations-dir ${locations_dir} \\
            --output-root dp_project/inputs \\
            --config-path ${params.feature_extraction_config} \\
            --channel-map "1=DNA,2=RNA,3=ER,4=AGP,5=Mito" \\
            --label-value "DMSO"

        test -f dp_project/inputs/metadata/index.csv

        # Copy config
        cp ${params.feature_extraction_config} dp_project/inputs/config/config.json

        # Link model weights if provided
        if [ -n "${params.feature_extraction_weights}" ] && [ -f "${params.feature_extraction_weights}" ]; then
            ln -s ${params.feature_extraction_weights} dp_project/outputs/cell_painting/checkpoint/
        fi

        # Run DeepProfiler
        python -m deepprofiler \\
            --root dp_project/ \\
            --config config.json \\
            --exp cell_painting \\
            --gpu 0 \\
            profile

        # Move output features to standard location
        cp -r dp_project/outputs/cell_painting/features/* features/ 2>/dev/null || true
        """
```

- [ ] **Step 4: Run architecture test**

Run:

```bash
pytest tests/test_nextflow_architecture_smoke.py::test_feature_extract_builds_deepprofiler_metadata_contract -q
```

Expected: pass.

- [ ] **Step 5: Run focused tests**

Run:

```bash
pytest tests/test_nextflow_chunking.py tests/test_nextflow_architecture_smoke.py tests/test_eddie_runtime_config.py tests/test_containers.py -q
```

Expected: all pass.

- [ ] **Step 6: Commit**

Run:

```bash
git add nextflow/modules/feature_extract.nf tests/test_nextflow_architecture_smoke.py
git commit -m "wire DeepProfiler metadata bridge into Nextflow"
```

---

### Task 4: Validate On Eddie With Loop 440 Resume

**Files:**
- Remote mirror: `/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2`
- Scratch root: `/exports/eddie/scratch/mharvey2/cptools2-ai-update`
- Local docs after run: `.claude/plans/phase-2.8-eddie-container-validation.md`, `TODOs.md`

- [ ] **Step 1: Push local branch**

Run:

```bash
git push origin ai-update
```

Expected: GitHub branch advances to the new commits.

- [ ] **Step 2: Fast-forward Eddie mirror**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 && git -c safe.directory=/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 pull --ff-only origin ai-update && git -c safe.directory=/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 rev-parse --short HEAD"
```

Expected: Eddie mirror fast-forwards. `containers/` remains untracked and present.

- [ ] **Step 3: Launch Loop 440 with resume**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk 'run_id=loop440-smoke-$(date +%Y%m%d-%H%M%S); log_dir=/exports/eddie/scratch/mharvey2/cptools2-ai-update/logs/$run_id; mkdir -p "$log_dir"; status_file="$log_dir/status.txt"; log_file="$log_dir/launcher.log"; ( cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 && . config/eddie_env.sh && cptools2 pipeline config/loop440-eddie-smoke.yaml --resume > "$log_file" 2>&1; printf "%s\n" "$?" > "$status_file" ) & pid=$!; printf "RUN_ID=%s\nPID=%s\nLOG=%s\nSTATUS=%s\n" "$run_id" "$pid" "$log_file" "$status_file"'
```

Expected: Nextflow resumes cached index/chunk/Cellpose if hashes are reusable, then retries `FEATURE_EXTRACT`.

- [ ] **Step 4: Poll qstat and logs**

Run every 60 seconds while active:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "qstat -u mharvey2 | tail -n +3 || true"
```

Then inspect the newest launcher log:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "latest=\$(ls -td /exports/eddie/scratch/mharvey2/cptools2-ai-update/logs/loop440-smoke-* | head -1); echo \$latest; tail -160 \$latest/launcher.log; cat \$latest/status.txt 2>/dev/null || true"
```

Expected: `FEATURE_EXTRACT` creates `dp_project/inputs/metadata/index.csv`; if DeepProfiler fails later, the failure should be about model/checkpoint/config semantics, not missing metadata.

- [ ] **Step 5: Capture evidence**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "find /exports/eddie/scratch/mharvey2/cptools2-ai-update/results/loop440 -maxdepth 4 -type f \( -name 'trace*' -o -name 'report*' -o -name 'timeline*' -o -name 'params*.json' \) -printf '%p\t%s\n' | sort"
```

Expected: params, trace, report, and timeline are present under scratch. If the run completes, features should be under `results/loop440/tiny-plate-001/features/`.

- [ ] **Step 6: Update docs with outcome**

Update `.claude/plans/phase-2.8-eddie-container-validation.md`:

```markdown
- Loop 440 DeepProfiler metadata bridge: generated `dp_project/inputs/metadata/index.csv`
  and `dp_project/inputs/locations/<plate>/<well>-f<site>-Nuclei.csv` from the chunk
  manifest and Cellpose centroids before invoking DeepProfiler.
- Eddie rerun status: <passed or failed with exact blocker>.
```

Update `TODOs.md`:

```markdown
- [x] Add DeepProfiler metadata bridge for Nextflow chunk manifests and Cellpose locations.
- [ ] Capture final Loop 440 feature extraction evidence after Eddie rerun.
```

- [ ] **Step 7: Commit docs**

Run:

```bash
git add .claude/plans/phase-2.8-eddie-container-validation.md TODOs.md
git commit -m "document DeepProfiler metadata bridge validation"
git push origin ai-update
```

---

## Self-Review

**Spec coverage:** The plan addresses the observed missing `index.csv`, moves conversion into reusable Python, wires Nextflow to call it, validates locally, syncs Eddie, reruns Loop 440, and records evidence.

**Placeholder scan:** No implementation step uses TBD/TODO/fill-in language. The only variable outcome is the Eddie rerun result, which must be recorded from actual evidence.

**Type consistency:** The helper accepts `chunk_manifest`, `locations_dir`, `output_root`, `channel_map`, and `label_value`; tests and CLI use the same names. The generated DeepProfiler columns match the current config channel names and label field.

**Known risk:** DeepProfiler may next fail on model checkpoint availability or config/model assumptions after metadata is fixed. That would be a later, different blocker and should not be hidden by this bridge.
