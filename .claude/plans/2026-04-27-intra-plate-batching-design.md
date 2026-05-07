# Intra-Plate Batching Design

## Goal

Restore cptools2's SGE-native strength of splitting large plates into many parallel jobs, while making Nextflow the owner of orchestration, dependencies, resources, retries, and resume behaviour.

## Design Summary

cptools2 will use a two-layer batching model.

The outer layer remains a Python/cptools2 responsibility: inspect available scratch, estimate plate sizes, and choose a scratch-safe batch of whole plates to stage. This protects Eddie scratch and preserves the proven 75% utilisation plus overhead-buffer model from the SGE-native implementation.

The inner layer becomes a Nextflow responsibility: after a selected plate has been staged to scratch, Nextflow runs small cptools2/parserix-backed helper scripts to index ImageXpress files, group images into complete image sets, split those image sets into chunks of 96, and fan out one task per chunk for chunkable stages.

This keeps the old ideas but changes the execution model. The new pipeline should not generate SGE command files or use SGE arrays directly. Nextflow replaces SGE arrays.

## Pipeline Shape

```text
YAML config
  -> cptools2 CLI parses config and resolves containers
  -> cptools2 computes scratch-safe plate batches
  -> cptools2 launches one Nextflow run per selected plate batch
  -> Nextflow stages whole plate(s) from DataStore to scratch
  -> Nextflow runs parserix-backed indexing script per staged plate
  -> Nextflow creates 96-image-set chunk manifests
  -> Nextflow runs chunkable stages as many independent SGE jobs
  -> Nextflow stages outputs back from scratch
```

## Responsibility Boundaries

### cptools2 CLI

- Parse YAML.
- Enforce `stage_data: true` for DataStore-backed runs.
- Resolve container paths.
- Check scratch space.
- Compute outer plate batches.
- Pass the active batch's plate list into each Nextflow invocation.
- Pass `chunk_size`, default `96`, into Nextflow params.
- Pass a chunk subset limit, such as `max_chunks`, for Eddie smoke tests.

### Nextflow

- Stage selected whole plates to scratch.
- Run the image-set indexing process.
- Run the chunk creation process.
- Submit chunk-level CPU/GPU jobs.
- Manage dependencies, retries, resume, queues, and resources.
- Stage outputs back to the configured destination.

### cptools2 Helper Scripts

These scripts are called by Nextflow processes. They do not submit jobs.

- Use `parserix` to parse ImageXpress paths and filenames.
- Produce an image-set index with plate, well, site, channel, and scratch-local image path.
- Validate expected channels per image set.
- Split image sets into chunk manifests.
- Create tool-specific inputs where needed, such as CellProfiler LoadData-compatible CSVs or DeepProfiler input metadata.
- Fail clearly when required channels are missing.

## Chunking Contract

The chunking unit is an image set, not a raw image file.

One image set means one plate/well/site with all expected channels. For Cell Painting this normally means the five expected channel images stay together.

Default chunk size: `96` image sets.

Required manifest columns:

| Column | Purpose |
|--------|---------|
| `plate` | Plate identifier |
| `well` | Parsed well id |
| `site` | Parsed site/field id |
| `channel` | Parsed channel name or index |
| `image_path` | Scratch-local image path |
| `image_set_id` | Stable `plate/well/site` image-set id |
| `chunk_id` | Stable chunk id |

Every downstream output must retain enough metadata to recover `plate`, `well`, `site`, and `image_path`.

## Stage Behaviour

### Illumination Calculate

Keep plate-level initially. This stage may need whole-plate context, and preserving scientific correctness matters more than forcing parallelism too early.

### Illumination Apply

Chunkable. It should consume a chunk manifest and the plate-level illumination functions.

### Segmentation

For this AI-focused branch, Cellpose is required when running AI feature extraction paths. Cellpose segmentation should run as a chunk-level GPU stage before DeepProfiler, Cell-DINO, uniDINO, or related feature extractors.

For a pure CellProfiler-only baseline, Cellpose is not required. The pipeline may retain a CellProfiler segmentation option for non-AI workflows, but this branch should prioritise the Cellpose-backed AI path.

### Feature Extraction

Chunkable. DeepProfiler should be the first AI extractor. Cell-DINO and uniDINO follow after the DINO model documentation and licensing notes are incorporated.

Feature extraction outputs are extraction products only. Downstream aggregation and hit calling are out of scope for this phase.

## Eddie Resource Rules

- Staging jobs use `-q staging` and no PE.
- CPU chunk jobs use the default sharedmem PE only when `cpus > 1`.
- GPU chunk jobs use `-q gpu -l gpu=1` and Singularity `--nv`.
- Add conservative throttling for GPU chunk tasks so one plate cannot flood the GPU queue.
- Keep Nextflow `queueSize` conservative until Loop 280 measures real scheduler behaviour.

## Validation Plan

1. Local/unit test: image-set grouping keeps all channels for each well/site together.
2. Local/unit test: 97 image sets create two chunks with default chunk size 96.
3. Local Nextflow config smoke: chunking params parse under the test and Eddie profiles.
4. Eddie subset run: stage one example-screen plate and run the first few chunks only using `max_chunks`.
5. Eddie scaling run: prove many SGE jobs are submitted for one staged plate.
6. Representative run: example-screen plate `example-plate-001` runs through the AI-focused path with Cellpose plus DeepProfiler.

## Non-Goals

- Reusing legacy SGE command generation.
- Staging per chunk from DataStore.
- Full DINO implementation in this phase.
- Downstream aggregation or analysis.
- Replacing the legacy `generate` command.

## Decisions Locked

- Use whole-plate staging after outer scratch-safe plate batching.
- Use Nextflow-owned intra-plate fan-out.
- Use cptools2/parserix helper scripts inside Nextflow processes for ImageXpress domain logic.
- Use `96` image sets as the default chunk size.
- Require Cellpose for AI feature extraction paths.
- Do not require Cellpose for pure CellProfiler-only baseline workflows.
