# Design Spec: cptools2 Container-Based Multi-Stage Pipeline

**Date**: 2026-04-06
**Branch**: `ai-update`
**Status**: Approved design, pending implementation planning

---

## 1. Goals

Transform cptools2 from a CellProfiler-only HPC job generator into a general-purpose, container-based pipeline orchestrator for Cell Painting workflows on Eddie. The system must:

1. **Replace conda with Singularity containers** for all tool execution (CellProfiler, DeepProfiler, Cellpose, and future tools like DINOv2)
2. **Support a multi-stage pipeline**: illumination correction → segmentation → feature extraction
3. **Be extensible**: adding a new model/tool should require only a manifest entry + config change, not Python code changes
4. **Work on Eddie HPC** with proper SGE resource requests (`h_rss`, GPU PEs, staging queues, job chaining via `hold_jid`)

### Strategy

The `master` branch retains the existing conda-based workflow. All container and multi-stage work happens on the `ai-update` branch as a clean rebuild. No backward-compatibility shims — this branch fully commits to containers.

---

## 2. Container Build & Deploy

### 2.1 Three initial containers

| Container | Source | GPU | Approx Size |
|---|---|---|---|
| `cellprofiler_4.2.8.sif` | `docker://cellprofiler/cellprofiler:4.2.8` | No | ~1.5 GB |
| `deepprofiler_1.0.sif` | Custom `Dockerfile.deepprofiler` (TF 2.5.3-gpu) | Yes | ~8-10 GB |
| `cellpose_sam_1.0.sif` | Custom `Dockerfile.cellpose` (CUDA 11.8 + PyTorch) | Yes | ~5-7 GB |

### 2.2 Build workflow

```
Local (WSL2/Docker)                    Eddie (HPC)
───────────────────                    ───────────────
1. docker pull / docker build          4. singularity build (SGE job, h_rss=16G)
2. docker run [--gpus all] (test)         Set SINGULARITY_TMPDIR to scratch
3. docker save | gzip                  5. qlogin → singularity exec (validate)
   rsync -avzP → Eddie ──────────────►    GPU: -pe gpu-a100 1 -l gpus=1, --nv
                                       6. Update cptools2_containers.json manifest
```

### 2.3 Dockerfiles stored in repo

```
cptools2/dockerfiles/
  Dockerfile.deepprofiler
  Dockerfile.cellpose
  README.md              # Build instructions
```

CellProfiler uses the official Docker Hub image directly (no custom Dockerfile).

### 2.4 Storage on Eddie

All containers stored in Eddie group space (persistent, shared):
```
/exports/cmvm/eddie/scs/groups/chandranlabs/containers/
  cellprofiler_4.2.8.sif
  deepprofiler_1.0.sif
  cellpose_sam_1.0.sif
  cptools2_containers.json
```

### 2.5 Container manifest (cptools2_containers.json)

The manifest is the source of truth for what containers are available. It is role-based and extensible:

```json
{
    "_updated": "2026-04-06",
    "containers": {
        "cellprofiler": {
            "image": "cellprofiler_4.2.8.sif",
            "version": "4.2.8",
            "source": "docker://cellprofiler/cellprofiler:4.2.8",
            "gpu": false,
            "verified": false
        },
        "deepprofiler": {
            "image": "deepprofiler_1.0.sif",
            "version": "1.0",
            "source": "Dockerfile.deepprofiler",
            "gpu": true,
            "verified": false
        },
        "cellpose": {
            "image": "cellpose_sam_1.0.sif",
            "version": "1.0",
            "source": "Dockerfile.cellpose",
            "gpu": true,
            "verified": false
        }
    }
}
```

**Adding a new tool (e.g., DINOv2):**
1. Build and deploy the `.sif` to the container directory
2. Add a `"dinov2"` entry to the manifest JSON
3. Reference `tool: dinov2` in the YAML config's feature extraction section

No Python code changes needed.

### 2.6 Eddie-specific build constraints

- **Never build on login node** — submit as SGE job with `-l h_rss=16G`
- **Set SINGULARITY_TMPDIR** to scratch — default `/tmp` is too small for GPU container extraction
- **GPU validation** requires `-pe gpu-a100 1 -l gpus=1` (Eddie-specific PE syntax) and `--nv` flag
- **Use rsync -avzP** for large archive transfers (resumable)

---

## 3. cptools2 Code Integration

### 3.1 `containers.py` changes

The existing module is mostly ready. Changes needed:

- **Allow unknown roles from manifest**: Currently `_image_name_for_role()` raises `ValueError` for roles not in `DEFAULT_CONTAINERS`. Change to accept any role found in the manifest.
- **Add GPU flag lookup**: `is_gpu_container(role)` reads the manifest `gpu` field. Used by command generators to add `--nv` flag.
- **Remove hardcoded `DEFAULT_CONTAINERS`** or demote to fallback-only. The manifest is authoritative on `ai-update`.

### 3.2 `generate_scripts.py` changes

**`load_module_text()`** — simplified, no parameters:
```python
def load_module_text():
    return "module load singularity\n"
```

**`_create_analysis_script()`** — use `SafePathScript` instead of scissorhands `AnalysisScript` (which emits conda activation). Commands are already base64-encoded `singularity exec` invocations.

**Memory specification** — always use `h_rss` (not `h_vmem`). Per-stage defaults:
- Illumination calculate: `-pe sharedmem 4 -l h_rss=16G` (64 GB total)
- Illumination apply: `-l h_rss=4G` (single slot)
- Segmentation: `-l h_rss=4G` (single slot)
- Feature extraction: `-pe gpu-a100 1 -l gpus=1 -l h_rss=16G`

### 3.3 `commands.py` changes

**`cp_command()`** gains container parameters:
```python
def cp_command(pipeline, load_data, output_location, container_path, gpu=False, bind_paths=None):
    base = f'cellprofiler -r -c -p "{pipeline}" --data-file="{load_data}" -o "{output_location}"'
    nv_flag = "--nv " if gpu else ""
    bind_str = " ".join(f"--bind {p}:{p}" for p in (bind_paths or ["/exports/eddie/scratch/$USER"]))
    return f'singularity exec {nv_flag}{bind_str} "{container_path}" {base}'
```

**New `container_command()`** — generic wrapper for non-CellProfiler tools:
```python
def container_command(container_path, command, gpu=False, bind_paths=None):
    """Wrap any command in singularity exec with appropriate flags."""
    nv_flag = "--nv " if gpu else ""
    bind_str = " ".join(f"--bind {p}:{p}" for p in (bind_paths or ["/exports/eddie/scratch/$USER"]))
    return f'singularity exec {nv_flag}{bind_str} "{container_path}" {command}'
```

### 3.4 `parse_yaml.py` changes

Expanded config namedtuple fields:
- `container_path` — resolved CellProfiler container (existing, now mandatory on ai-update)
- `containers_config` — optional per-role overrides from YAML
- `pipeline_stages` — list of stages to run (for `pipeline` command)
- `feature_extraction_config` — tool name + parameters for feature extraction stage

`check_yaml_args()` updated to accept new keys: `containers`, `pipeline_stages`, `feature_extraction`, `segmentation`, `illumination`.

### 3.5 YAML config evolution

```yaml
# Existing fields (retained):
experiment: /path/to/experiment
chunk: 96
pipeline: /path/to/pipeline.cppipe
location: /path/to/scratch/$USER/project/outputs
commands location: /path/to/scratch/$USER/project/commands

# Container config (new, optional — defaults from env var + manifest):
container_path: /exports/.../containers/cellprofiler_4.2.8.sif
# OR: set CPTOOLS2_CONTAINER_DIR env var

# Multi-stage pipeline (new, for `cptools2 pipeline`):
pipeline_stages:            # if omitted, runs all stages in order
  - illum_calculate
  - illum_apply
  - segmentation
  - feature_extraction

feature_extraction:
  tool: deepprofiler    # role name → looked up in manifest
  # Future: tool: dinov2
```

---

## 4. Multi-Stage Pipeline Architecture

### 4.1 Stage abstraction

Each stage is a module under `cptools2/stages/` implementing a common interface:

```
cptools2/
  stages/
    __init__.py
    base.py              # Base class / shared logic
    illum_calculate.py   # Stage 1: per-plate illumination functions (.npy)
    illum_apply.py       # Stage 2: apply correction → 16-bit PNG
    segmentation.py      # Stage 3: nuclear segmentation → centroid CSVs
    feature_extract.py   # Stage 4: feature embeddings (role-agnostic)
    validate.py          # Post-stage validation checks
```

Each stage defines:
- `role` — which container to use (manifest lookup)
- `gpu` — whether `--nv` is needed
- `memory` / `slots` / `queue` — SGE resource requirements
- `partition()` — how to split work (by plate, by chunk, etc.)
- `generate_commands()` — command strings for each array task
- `generate_script()` — SGE job script with dependencies
- `validate()` — post-run completeness checks

### 4.2 Pipeline chain

```
illum_calculate ──► illum_apply ──► segmentation ──► feature_extraction
  CP, CPU, 64G       CP, CPU, 4G    CP, CPU, 4G      DP/DINO, GPU, 16G
  per-plate           per-chunk      per-chunk         per-plate
```

Dependencies expressed via `-hold_jid <stage_job_name>` (job names, not IDs — robust on Eddie).

### 4.3 Key module changes for multi-stage support

**`splitter.py`** — add `split_by_plate(plate_store)`:
- Groups all images for each plate into a single partition
- Used by `illum_calculate` (CellProfiler's "All" mode needs every image in a plate)
- Existing `split()` unchanged

**`loaddata.py`** — add `illum_dir` parameter to `create_loaddata()`:
- When provided, appends `FileName_Illum_<Channel>` and `PathName_Illum_<Channel>` columns per channel
- Points to `.npy` files from Stage 1 output
- Warns if expected `.npy` files don't exist

**Pipeline templates** in `cptools2/templates/`:
- `illum_calculate.cppipe` — CorrectIlluminationCalculate, "All" mode, median filter, 5 channels
- `illum_apply.cppipe` — CorrectIlluminationApply, division method, 16-bit PNG output
- `nuclear_segmentation.cppipe` — IdentifyPrimaryObjects on DNA channel, export centroid CSVs

DeepProfiler uses a `config.json` template (not a `.cppipe`).

### 4.4 CLI structure

```bash
# Individual stage commands (debugging, reruns):
cptools2 illum calculate config.yml
cptools2 illum apply config.yml
cptools2 segment config.yml
cptools2 extract config.yml
cptools2 illum validate --stage 1 --output /path/to/illum_functions

# Full pipeline orchestrator:
cptools2 pipeline config.yml
# → generates all stages with hold_jid chains
# → writes a master submit script

# Existing commands (updated for containers):
cptools2 generate config.yml    # existing workflow, now uses singularity
cptools2 join --location ...    # unchanged
```

---

## 5. Implementation Phases

### Phase 0 — Container Build & Deploy (parallel with Phase 1)

Build 3 containers locally, test, transfer to Eddie, convert to `.sif`, validate, write manifest.

**Deliverables:**
- `cptools2/dockerfiles/Dockerfile.deepprofiler`
- `cptools2/dockerfiles/Dockerfile.cellpose`
- 3 validated `.sif` files on Eddie
- `cptools2_containers.json` manifest in container directory

### Phase 1 — Container Integration into cptools2 (parallel with Phase 0)

Wire `containers.py` into `generate_scripts.py` and `commands.py`. Remove conda paths.

**Files changed:**
- `generate_scripts.py` — `load_module_text()` simplified; analysis scripts use `SafePathScript` + `singularity exec`
- `commands.py` — `cp_command()` wraps in `singularity exec`; new `container_command()` generic
- `containers.py` — allow unknown roles from manifest; add GPU flag lookup
- `parse_yaml.py` — new config fields; `container_path` mandatory
- `__main__.py` — pass container through to job generation
- Tests updated to expect singularity output

**Validation:**
- All existing tests pass with singularity-based output
- `cptools2 generate config.yml` produces scripts with `module load singularity` and `singularity exec`
- Single-plate test on Eddie produces identical output to previous conda run

### Phase 2 — Illumination Correction Stages

**Files created:**
- `cptools2/stages/__init__.py`
- `cptools2/stages/base.py`
- `cptools2/stages/illum_calculate.py`
- `cptools2/stages/illum_apply.py`
- `cptools2/templates/illum_calculate.cppipe`
- `cptools2/templates/illum_apply.cppipe`

**Files changed:**
- `splitter.py` — add `split_by_plate()`
- `loaddata.py` — add `illum_dir` parameter
- `__main__.py` — register `illum` subcommand

**Validation:**
- `cptools2 illum calculate` generates per-plate partitions + SGE scripts
- Stage 1 dry-run on Eddie produces `.npy` illumination functions
- `cptools2 illum apply` generates LoadData CSVs with `.npy` columns + SGE scripts
- Stage 2 dry-run produces corrected 16-bit PNGs

### Phase 3 — Segmentation Stage

**Files created:**
- `cptools2/stages/segmentation.py`
- `cptools2/templates/nuclear_segmentation.cppipe`

**Files changed:**
- `__main__.py` — register `segment` subcommand

**Validation:**
- Generates centroid CSVs with `Nuclei_Location_Center_X/Y` columns
- Cell counts within expected range per well

### Phase 4 — Feature Extraction Stage

**Files created:**
- `cptools2/stages/feature_extract.py`
- `cptools2/templates/deepprofiler_config.json`

**Files changed:**
- `__main__.py` — register `extract` subcommand
- `commands.py` — `container_command()` used for DeepProfiler invocation

**Validation:**
- GPU job scripts use `-pe gpu-a100 1 -l gpus=1` and `--nv`
- DeepProfiler produces 672-dim `.npz` feature files
- Role is configurable — can swap `deepprofiler` for `dinov2` via config

### Phase 5 — Pipeline Orchestrator

**Files created:**
- `cptools2/pipeline.py` — orchestrates stage chain

**Files changed:**
- `__main__.py` — register `pipeline` subcommand

**Validation:**
- `cptools2 pipeline config.yml` generates all stages with correct `hold_jid` dependencies
- Master submit script chains all jobs
- End-to-end run on Eddie

---

## 6. Eddie-Specific Constraints (Reference)

These constraints are baked into the design above. Listed here for implementer reference.

| Constraint | Detail |
|---|---|
| Memory | Use `h_rss` not `h_vmem` (September 2025 change). Memory is per-slot. |
| GPU access | `-pe gpu-a100 N -l gpus=N` (Eddie-specific PE syntax). `--nv` flag essential. |
| Staging queue | DataStore access only via `-q staging`. Incompatible with `-pe sharedmem`. |
| Job chaining | Use `-hold_jid <job_name>` (names, not IDs — robust for array jobs). |
| Container builds | Never on login node. SGE job with `h_rss=16G`. Set `SINGULARITY_TMPDIR` to scratch. |
| Container storage | Group space (persistent, shared) — not scratch (auto-purged after 1 month). |
| Bind mounts | `--bind /exports/eddie/scratch/$USER` always required. DataStore paths only on staging nodes. |

---

## 7. Directory Structure (Target)

```
cptools2/
  __init__.py
  __main__.py              # CLI entry point — generate, join, illum, segment, extract, pipeline
  commands.py              # cp_command(), container_command()
  colours.py
  containers.py            # resolve_container_path(), validate, manifest read
  file_tools.py
  filelist.py
  generate_scripts.py      # SGE script generation (singularity-only)
  job.py
  loaddata.py              # create_loaddata() with illum_dir extension
  parse_yaml.py            # YAML config parsing
  pipeline.py              # Multi-stage orchestrator
  splitter.py              # split() + split_by_plate()
  utils.py
  container_manifest_template.json
  dockerfiles/
    Dockerfile.deepprofiler
    Dockerfile.cellpose
    README.md
  stages/
    __init__.py
    base.py
    illum_calculate.py
    illum_apply.py
    segmentation.py
    feature_extract.py
    validate.py
  templates/
    illum_calculate.cppipe
    illum_apply.cppipe
    nuclear_segmentation.cppipe
    deepprofiler_config.json
```
