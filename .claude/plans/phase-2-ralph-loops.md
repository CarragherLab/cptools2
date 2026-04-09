---
phase: 2
name: Eddie Deployment
plan: phase-2-eddie-deployment.md
status: in_progress
loops_total: 4
loops_done: 2
---

# Phase 2: Eddie Deployment — Containers, Staging, End-to-End

## Loop 210: Container Build & Deploy

```yaml
loop_id: 210
name: Container Build & Deploy
status: pending
type: infrastructure
key_outputs:
  - 3 .sif containers on Eddie at .../chandranlabs/cptools2/containers/
  - Container manifest deployed and verified
  - build_containers.sh parameterized for reuse
todos:
  - text: "Parameterize build_containers.sh: accept CONTAINER_DIR as first argument with default /exports/cmvm/eddie/scs/groups/chandranlabs/cptools2/containers"
    status: completed
    skill: null
    agent: null
  - text: "Remove dead eddie_container_dir param from nextflow/conf/containers.config"
    status: completed
    skill: null
    agent: null
  # AWAITING MANUAL EXECUTION — todos below require Eddie SSH or Docker daemon access
  - text: "Create cptools2/ directory on Eddie: ssh eddie 'mkdir -p /exports/cmvm/eddie/scs/groups/chandranlabs/cptools2/{containers,env,nextflow}'"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Build Docker images locally: docker pull cellprofiler:4.2.8, docker build deepprofiler:1.0, docker build cellpose-sam:1.0"
    status: pending
    skill: null
    agent: null
  - text: "Save Docker archives: docker save | gzip for all 3 images"
    status: pending
    skill: null
    agent: null
  - text: "Transfer archives to Eddie: rsync -avzP *.tar.gz to chandranlabs/cptools2/containers/"
    status: pending
    skill: null
    agent: null
  - text: "Submit SGE build job on Eddie: qsub build_containers.sh (converts docker-archive to .sif)"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Validate CellProfiler container: qlogin + singularity exec cellprofiler_4.2.8.sif cellprofiler --version"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Validate DeepProfiler container (GPU): qlogin -pe gpu-a100 1 + singularity exec --nv deepprofiler_1.0.sif TF GPU check"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Validate Cellpose-SAM container (GPU): singularity exec --nv cellpose_sam_1.0.sif torch.cuda check"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Deploy manifest: scp container_manifest_template.json to Eddie as cptools2_containers.json, update verified flags"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Run pytest tests/ -v locally and verify 140+ tests still pass after config changes"
    status: completed
    skill: null
    agent: null
```

## Loop 220: Staging + Batch Orchestration

```yaml
loop_id: 220
name: Staging + Batch Orchestration
status: completed
type: implementation
depends_on: [210]
key_outputs:
  - nextflow/modules/stage_in.nf and stage_out.nf
  - cptools2/batch.py with compute_plate_sizes, create_batches, get_scratch_quota
  - cmd_pipeline updated with subprocess.run batch loop
  - eddie.config staging label updated with time = 4.h
  - tests/test_batch.py with 6-7 unit tests
  - scripts/install_eddie.sh for shared env deployment
handoff_summary:
  done: "stage_in.nf, stage_out.nf, batch.py, test_batch.py (8 tests), scripts/install_eddie.sh created; __main__.py uses subprocess.run batch loop with scratch pre-flight; eddie.config staging time=4h; 148 tests pass."
  failed: ""
  needed: "Loop 230: end-to-end Eddie test — deploy via install_eddie.sh, run pipeline on real plate, validate output structure."
todos:
  - id: "loop-220-1"
    content: "Create nextflow/modules/stage_in.nf: STAGE_IN process with label 'staging', rsync --partial --timeout=300 from DataStore to scratch, output tuple(plate_id, staged_path)"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "nextflow/modules/stage_in.nf exists; contains STAGE_IN process with label 'staging', rsync --partial --timeout=300, and emits tuple(plate_id, staged_path)"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-220-2"
    content: "Create nextflow/modules/stage_out.nf: STAGE_OUT process with label 'staging', rsync --partial --timeout=300 results from scratch to DataStore output"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "nextflow/modules/stage_out.nf exists; contains STAGE_OUT process with label 'staging' and rsync --partial --timeout=300 to DataStore"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-220-3"
    content: "Update eddie.config: add time = '4.h' to withLabel: 'staging' block"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "nextflow/conf/eddie.config withLabel: 'staging' block contains time = '4.h'"
    status: completed
    complexity: low
    priority: high
  - id: "loop-220-4"
    content: "Update main.nf: include STAGE_IN and STAGE_OUT modules, wire into workflow (STAGE_IN before ILLUM_CALCULATE, STAGE_OUT after FEATURE_EXTRACT)"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "nextflow/main.nf includes STAGE_IN and STAGE_OUT; workflow calls STAGE_IN before ILLUM_CALCULATE and STAGE_OUT after FEATURE_EXTRACT"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-220-5"
    content: "Create cptools2/batch.py: migrate batch logic from job.py — compute_plate_sizes(input_dir), create_batches(plate_sizes, available_scratch) with 75% util / 30% overhead, get_scratch_quota() with lfs/df/YAML fallback chain"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "cptools2/batch.py exists; exports compute_plate_sizes, create_batches, get_scratch_quota; 75%/30% constants present; fallback chain implemented"
    status: completed
    complexity: high
    priority: high
  - id: "loop-220-6"
    content: "Update __main__.py cmd_pipeline: replace os.execvp with subprocess.run batch loop. Add scratch pre-flight check (warn at 80%). Add -profile eddie to nf_cmd. Skip completed batches on resume."
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "__main__.py cmd_pipeline uses subprocess.run; scratch pre-flight warns at 80%; -profile eddie in nf_cmd; completed batches skipped on resume"
    status: completed
    complexity: high
    priority: high
  - id: "loop-220-7"
    content: "Create scripts/install_eddie.sh: parameterized install script that creates cptools2/{containers,env,nextflow} structure, conda env, pip install, nextflow symlink, activate.sh"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "scripts/install_eddie.sh exists; accepts base dir param; creates containers/env/nextflow subdirs; installs conda env; creates activate.sh"
    status: completed
    complexity: medium
    priority: medium
  - id: "loop-220-8"
    content: "Create tests/test_batch.py: test create_batches (single plate fits, multi-batch, oversized plate, empty list), test get_scratch_quota fallbacks (mock lfs/df), test compute_plate_sizes with tmp_path"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "tests/test_batch.py exists; contains 6-7 test functions covering all specified cases; all tests pass"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-220-9"
    content: "Run pytest tests/ -v and verify 146+ tests pass (140 existing + 6 new batch tests)"
    skill: "verification-before-completion"
    agent: "ralph-loop-worker"
    outcome: "pytest exits 0; 146 or more tests collected and passing; no regressions in existing test suite"
    status: completed
    complexity: low
    priority: high
```

## Loop 230: Cellpose + DeepProfiler Bridge

> **Rewritten 2026-04-09** — previous scope ("end-to-end Eddie test") moved to Loop 240.
> New scope is all code + tests + docs for cellpose-SAM segmentation and DeepProfiler
> feature extraction bridge. All work green locally before Loop 240 touches Eddie.
> Source of truth: `.claude/plans/2026-04-09-cellpose-deepprofiler-design.md`.

```yaml
loop_id: 230
name: Cellpose + DeepProfiler Bridge
status: pending
type: implementation
depends_on: [220]
design_doc: .claude/plans/2026-04-09-cellpose-deepprofiler-design.md
key_outputs:
  - cptools2/cellpose_segmentation.py + cptools2/deepprofiler_index.py (importable libraries)
  - nextflow/bin/run_cellpose_segmentation.py + nextflow/bin/make_deepprofiler_index.py (thin CLI wrappers)
  - nextflow/modules/segmentation_cellpose.nf (new process with OOM retry)
  - Updated illum_calculate.cppipe + illum_apply.cppipe (w1/w2/w3 naming, 16-bit TIFF output)
  - Updated deepprofiler_config.json (channels w1..w5, file_format tiff, bits 16)
  - Updated main.nf (params.segmentation.tool switch + weights pre-flight)
  - Updated feature_extract.nf (calls index generator, bind-mounts weights)
  - Updated containers.py (resolve_weights_path + CPTOOLS2_WEIGHTS_DIR env var)
  - scripts/deploy_deepprofiler_weights.sh (Zenodo download + sha256 + parameterized group space deploy)
  - Updated install_eddie.sh (export CPTOOLS2_WEIGHTS_DIR in activate.sh)
  - docs/illumination_pipeline_setup.md + docs/deepprofiler_integration.md
  - 32+ new unit tests across 5 files (180+ tests total, all passing)
handoff_summary:
  done: ""
  failed: ""
  needed: ""
parallelization:
  lanes:
    - lane: A
      name: Cellpose path
      todos: [loop-230-a1, loop-230-a2, loop-230-a3, loop-230-a4]
      depends_on: []
    - lane: B
      name: DeepProfiler path
      todos: [loop-230-b1, loop-230-b2, loop-230-b3, loop-230-b4, loop-230-b5]
      depends_on: []
    - lane: C
      name: Templates + infra + main.nf wiring
      todos: [loop-230-c1, loop-230-c2, loop-230-c3, loop-230-c4, loop-230-c5, loop-230-c6, loop-230-c7, loop-230-c8]
      depends_on: [A, B]
todos:
  # ─── LANE A: Cellpose path ──────────────────────────────────────────────
  - id: "loop-230-a1"
    lane: A
    content: "Create cptools2/cellpose_segmentation.py importable library: group_images_by_site(corrected_dir, nuclei_channel) using parserix.parse.img_channel/well/site/plate_name; extract_centroids(mask) using scipy.ndimage.center_of_mass on non-zero integer labels; write_location_csv(centroids, output_path) with Nuclei_Location_Center_X/Y columns; run_cellpose_on_batch(images, model) with index-based defensive tuple unpacking (result[0] not destructuring)"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "cptools2/cellpose_segmentation.py exists with all 4 functions; imports parserix and scipy.ndimage; index-based tuple unpacking documented in code comment"
    status: pending
    complexity: high
    priority: high
  - id: "loop-230-a2"
    lane: A
    content: "Create tests/test_cellpose_segmentation.py with ~11 tests: group_images_by_site (happy path, channel filter, empty dir, mixed .tif/.tiff, partial site raises); extract_centroids (empty, single, multiple, non-contiguous labels); write_location_csv (format, parent dir creation); run_cellpose (defensive unpacking for 3-tuple and 4-tuple returns via mock). Cellpose itself is mocked since not installed in test env."
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "tests/test_cellpose_segmentation.py exists with 11 test functions; all pass; no import of cellpose package required (mocked)"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-230-a3"
    lane: A
    content: "Create nextflow/bin/run_cellpose_segmentation.py thin CLI wrapper (~30 lines): argparse for --corrected-dir, --output-dir, --nuclei-channel, --batch-size, --flow-threshold, --cellprob-threshold; imports from cptools2.cellpose_segmentation; early exit with clear error if zero files match *_w{nuclei_channel}.tiff; writes per-site CSVs at {output_dir}/locations/{Plate}/{Well}-{Site}-Nuclei.csv and integer-label masks at {output_dir}/masks/{Plate}_{Well}_s{Site}_Nuclei_mask.tiff"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "nextflow/bin/run_cellpose_segmentation.py exists; argparse help works; script is thin wrapper importing from cptools2 package"
    status: pending
    complexity: low
    priority: high
  - id: "loop-230-a4"
    lane: A
    content: "Create nextflow/modules/segmentation_cellpose.nf DSL2 process: labels 'segmentation' + 'gpu'; container params.containers.cellpose; errorStrategy 'retry'; maxRetries 2; memory { 32.GB * task.attempt }; input tuple(plate_id, corrected_dir); output tuple(plate_id, corrected_dir, segmentation_dir); calls run_cellpose_segmentation.py with params.segmentation.* args; publishDir {output_dir}/{plate_id}/segmentation/"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "nextflow/modules/segmentation_cellpose.nf exists; errorStrategy retry with memory scaling; output shape matches existing SEGMENTATION module (3-tuple)"
    status: pending
    complexity: medium
    priority: high

  # ─── LANE B: DeepProfiler path ──────────────────────────────────────────
  - id: "loop-230-b1"
    lane: B
    content: "Create cptools2/deepprofiler_index.py importable library: scan_corrected_dir(corrected_dir, channels) using parserix to group by (plate, well, site) and validate all channels present; build_index_rows(site_records, plate_map=None) that stubs Treatment='untreated' Replicate=1 if no plate_map or joins on Metadata_Well if provided; write_index_csv(rows, output_path, channel_columns) with explicit column order and relative paths; load_plate_map(path) thin pandas wrapper with FileNotFoundError + ValueError raising"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "cptools2/deepprofiler_index.py exists with all 4 functions; uses parserix for parsing; handles optional plate_map with honor-if-passed fail-if-broken semantics"
    status: pending
    complexity: high
    priority: high
  - id: "loop-230-b2"
    lane: B
    content: "Create tests/test_deepprofiler_index.py with ~11 tests: scan_corrected_dir (happy path, empty, partial site raises); build_index_rows (stubbed defaults, with plate_map, plate_map missing well stubs); write_index_csv (column order, channel count configurable, relative paths); load_plate_map (valid CSV, missing file raises FileNotFoundError, missing columns raises ValueError)"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "tests/test_deepprofiler_index.py exists with 11 test functions; all pass; uses tmp_path fixtures for file I/O tests"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-230-b3"
    lane: B
    content: "Create nextflow/bin/make_deepprofiler_index.py thin CLI wrapper: argparse for --corrected-dir, --output-path, --channels (default 'w1 w2 w3 w4 w5'), --plate-map (optional), --plate-id; imports from cptools2.deepprofiler_index; if --plate-map is passed honor it (fail loudly if broken), else stub defaults"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "nextflow/bin/make_deepprofiler_index.py exists; argparse help shows all flags; thin wrapper imports from cptools2 package"
    status: pending
    complexity: low
    priority: high
  - id: "loop-230-b4"
    lane: B
    content: "Update nextflow/modules/feature_extract.nf: call make_deepprofiler_index.py before python -m deepprofiler profile; bind-mount weights dir via containerOptions '--bind {weights_dir}:{weights_dir}:ro'; symlink weights into dp_project/outputs/cell_painting/checkpoint/ (matches existing experiment name at line 41); pass params.feature_extraction.plate_map_path through (optional)"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "nextflow/modules/feature_extract.nf calls make_deepprofiler_index.py in script block; bind-mount present in containerOptions; existing 3-tuple input contract preserved"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-230-b5"
    lane: B
    content: "Update cptools2/templates/deepprofiler_config.json: change channels to ['w1','w2','w3','w4','w5']; file_format to 'tiff'; bits to 16; label_field to 'Treatment'; control_value to 'untreated'; checkpoint to 'combinedset_cellsout_e30.hdf5' (filename only, no path — path composed from CPTOOLS2_WEIGHTS_DIR). Create tests/test_deepprofiler_config.py with 4 tests (valid JSON, channels w-numbered, file_format tiff, bits 16)"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "cptools2/templates/deepprofiler_config.json updated; tests/test_deepprofiler_config.py exists with 4 passing tests"
    status: pending
    complexity: low
    priority: high

  # ─── LANE C: Templates + infra + wiring (depends on A + B) ──────────────
  - id: "loop-230-c0"
    lane: C
    content: "Inspect user-provided CellProfiler illumination pipeline at U:\\Datastore\\IGMM\\Drug-Discovery\\Mungo\\CellProfiler\\IlluminationCorrection (READ-ONLY — never modify the original). Document: (1) which modules are used (Images/Metadata/NamesAndTypes vs LoadData), (2) filename regex pattern, (3) channel naming convention (w1..w5 vs DNA/RNA/etc.), (4) number of channels, (5) SaveImages format/suffix settings. Report findings to the user: if it matches our w-number convention, note that; if it diverges, flag the specific diffs and ask whether to (a) use the user's pipeline verbatim and adjust our Nextflow process, (b) create a corrected copy in cptools2/templates/ for Loop 230 template work, or (c) rewrite the templates from scratch per the design doc. Block loop-230-c1 and loop-230-c2 until the user answers."
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "Inspection report written (either inline in handoff or as dev_docs note); divergence diffs listed if any; user decision recorded on how to proceed with templates"
    status: pending
    complexity: low
    priority: high
    blocks: [loop-230-c1, loop-230-c2]
  - id: "loop-230-c1"
    lane: C
    content: "Update cptools2/templates/illum_calculate.cppipe: change NamesAndTypes assignment rules from (metadata does Channel 'DNA'/'RNA'/'ER'/'AGP'/'Mito') to (metadata does Channel 'w1'/'w2'/'w3'/'w4'/'w5'); change assigned image names from DNA/RNA/ER/AGP/Mito to w1/w2/w3/w4/w5; update CorrectIlluminationCalculate modules to use wN input names and produce IllumW{N} outputs; keep per-plate grouping and 200 smoothing filter size unchanged. (Blocked by loop-230-c0 outcome — user may redirect us to copy-adapt their pipeline instead)"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "cptools2/templates/illum_calculate.cppipe has 5 NamesAndTypes rules matching w1..w5; 5 CorrectIlluminationCalculate modules producing IllumW1..IllumW5"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-230-c2"
    lane: C
    content: "Update cptools2/templates/illum_apply.cppipe: change NamesAndTypes rules to match w1..w5 metadata; update CorrectIlluminationApply modules to consume w1..w5 + IllumW1..IllumW5; update SaveImages modules to save as 16-bit TIFF with 'From image filename' + NO suffix (preserve input filename stem); file format 'tiff' for all 5 SaveImages modules"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "cptools2/templates/illum_apply.cppipe SaveImages outputs {stem}.tiff preserving w-number; 5 CorrectIlluminationApply modules; loaddata from workdir symlinks via illum_functions/"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-230-c3"
    lane: C
    content: "Extend cptools2/containers.py: add resolve_weights_path(yaml_dict=None) function mirroring resolve_container_dir(); priority 1 YAML feature_extraction.weights_path; priority 2 CPTOOLS2_WEIGHTS_DIR env var; priority 3 None (legacy); expands $USER and ~. Add 4 tests to tests/test_containers.py covering all 4 cases"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "cptools2/containers.py exports resolve_weights_path; tests/test_containers.py has 4 new tests; all pass"
    status: pending
    complexity: low
    priority: high
  - id: "loop-230-c4"
    lane: C
    content: "Update nextflow/main.nf: add params.segmentation = [tool: 'cellpose', model: 'cpsam', nuclei_channel: 1, batch_size: 8, flow_threshold: 0.4, cellprob_threshold: 0.0] defaults; add params.feature_extraction = [weights_path: null, plate_map_path: null] defaults; include SEGMENTATION_CELLPOSE module; dispatch on params.segmentation.tool; add weights pre-flight check that fails fast if weights_path null and feature_extract stage enabled"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "nextflow/main.nf has params.segmentation and params.feature_extraction defaults; SEGMENTATION_CELLPOSE included; tool switch dispatches correctly; pre-flight check present"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-230-c5"
    lane: C
    content: "Extend tests/test_parse_yaml.py with 3 new tests: segmentation dict (tool/model/nuclei_channel/diameter/batch_size) passes through to params.json; feature_extraction.weights_path passes through; segmentation cellpose keys accepted by check_yaml_args without error"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "tests/test_parse_yaml.py has 3 new test functions; all pass"
    status: pending
    complexity: low
    priority: high
  - id: "loop-230-c6"
    lane: C
    content: "Create scripts/deploy_deepprofiler_weights.sh: parameterized bash script accepting list of target base dirs; defines CHECKPOINT_FILENAME=combinedset_cellsout_e30.hdf5 as single source of truth; CHECKPOINT_URL=https://zenodo.org/record/7114558/files/...; CHECKPOINT_SHA256 for verification; downloads to /tmp once, verifies sha256, copies to {BASE_DIR}/cptools2/models/ for each target; idempotent (skip if target exists with matching sha256); default targets chandranlabs + Drug-Discovery if no args. Update scripts/install_eddie.sh activate.sh template to export CPTOOLS2_WEIGHTS_DIR=${_CPTOOLS2_DIR}/models"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "scripts/deploy_deepprofiler_weights.sh exists, parameterized, idempotent; scripts/install_eddie.sh activate.sh exports CPTOOLS2_WEIGHTS_DIR"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-230-c7"
    lane: C
    content: "Create docs/illumination_pipeline_setup.md: how the illum templates work module-by-module; adapting for 3/4/5/6 channel assays (add/remove NamesAndTypes rules and Calculate/Apply/Save modules); filename convention requirements (w-number in the _w{N} position is load-bearing); filename contract section warning users who customize SaveImages that cellpose depends on the exact pattern; local testing with Docker. Refactor from dev_docs/new_pipeline_focus/guide_01_illumination_correction.md (gitignored source)"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "docs/illumination_pipeline_setup.md exists; covers 3/4/5/6 channel adaptation; documents filename contract explicitly"
    status: pending
    complexity: medium
    priority: medium
  - id: "loop-230-c8"
    lane: C
    content: "Create docs/deepprofiler_integration.md: DeepProfiler project layout; index.csv contract with required columns; optional plate_map format (user provides CSV with Metadata_Well, Treatment, Replicate columns); weights location (CPTOOLS2_WEIGHTS_DIR env var or YAML); guide_03 parquet assembly snippet for downstream use. Include a 'known gaps' section listing pycytominer integration as future work"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "docs/deepprofiler_integration.md exists; documents index.csv, plate_map, weights; includes parquet assembly example"
    status: pending
    complexity: medium
    priority: medium
  - id: "loop-230-c9"
    lane: C
    content: "Run pytest tests/ -v and verify all tests pass: 148 existing + 32 new = 180+ tests. No regressions. Run ruff + black + isort + pre-commit run --all-files on all new Python files"
    skill: "verification-before-completion"
    agent: "ralph-loop-worker"
    outcome: "pytest exits 0 with 180+ tests collected and passing; ruff/black/isort pass clean; git status shows only intended changes"
    status: pending
    complexity: low
    priority: high
```

## Loop 240: End-to-End Eddie Validation

> **New loop, 2026-04-09** — this is the scope that was originally Loop 230
> ("end-to-end Eddie test"), moved to 240 after Loop 230 was rewritten as the
> Cellpose + DeepProfiler bridge code work. All code from Loop 230 is green
> locally before this loop touches Eddie's SGE queue.

```yaml
loop_id: 240
name: End-to-End Eddie Validation
status: pending
type: validation
depends_on: [230]
design_doc: .claude/plans/2026-04-09-cellpose-deepprofiler-design.md
key_outputs:
  - cptools2 deployed to Eddie via install_eddie.sh
  - DeepProfiler weights deployed via deploy_deepprofiler_weights.sh to both group spaces
  - Test YAML config validated via --dry-run
  - Illum stage outputs verified (IllumW{N}.npy + corrected TIFFs)
  - Cellpose segmentation outputs verified (masks + DeepProfiler-format location CSVs)
  - DeepProfiler feature outputs verified (.npz files, shape (n_cells, 672))
  - Full pipeline end-to-end run complete
  - Retrospective written with job IDs + wall times + gotchas
handoff_summary:
  done: ""
  failed: ""
  needed: ""
todos:
  - id: "loop-240-1"
    content: "Deploy cptools2 to Eddie via install_eddie.sh /exports/cmvm/eddie/scs/groups/chandranlabs. Verify activate.sh created, conda env installs, cptools2 --version works, CPTOOLS2_CONTAINER_DIR and CPTOOLS2_WEIGHTS_DIR both exported"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "install_eddie.sh exits 0; source activate.sh sets both env vars; cptools2 --version prints 0.3.0 or newer"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-240-2"
    content: "Deploy DeepProfiler weights via scripts/deploy_deepprofiler_weights.sh. Verify combinedset_cellsout_e30.hdf5 exists at chandranlabs/cptools2/models/ and Drug-Discovery/cptools2/models/ with correct sha256"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "Both target paths contain the 20MB .hdf5 file with matching sha256; deploy script exits 0; re-running is idempotent (no re-download)"
    status: pending
    complexity: low
    priority: high
  - id: "loop-240-3"
    content: "Identify a specific small test plate on DataStore (~100 images, one cell line, known-good). Record the path and plate characteristics for the test config"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "Test plate path documented; plate has N wells × M sites × 5 channels in w1..w5 naming"
    status: pending
    complexity: low
    priority: high
  - id: "loop-240-4"
    content: "Create test YAML config on Eddie pointing at the test plate. Include segmentation.tool=cellpose, segmentation.nuclei_channel=1 (or whichever w-number has the nuclei stain), feature_extraction.weights_path=null (resolve from env), no plate_map. Container_path resolves to chandranlabs/cptools2/containers/"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "test_config.yaml exists on Eddie; cptools2 pipeline test_config.yaml --dry-run exits 0; params.json has resolved container paths, weights path, nextflow-friendly structure"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-240-5"
    content: "Run illum-only stage: cptools2 pipeline test_config.yaml --stages illum. Wait for SGE jobs (sharedmem PE, CPU only). Verify output at output_dir/{plate_id}/illum_functions/ (5 .npy files) and output_dir/{plate_id}/corrected_images/ (wells × sites × 5 .tiff files)"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "All SGE jobs exit_status=0; 5 IllumW{N}.npy files present; corrected TIFFs match expected wells × sites × 5 count; filenames follow w-number convention"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-240-6"
    content: "Run cellpose segmentation stage: cptools2 pipeline test_config.yaml --stages segmentation. Verify gpu-a100 PE allocation, --nv flag present in job script, one GPU allocated. Check output at output_dir/{plate_id}/segmentation/masks/ (one .tiff per site) and output_dir/{plate_id}/segmentation/locations/{plate}/ (one {well}-{site}-Nuclei.csv per site with Nuclei_Location_Center_X/Y columns)"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "SGE GPU job runs on gpu-a100 PE; segmentation masks exist; DeepProfiler-format location CSVs exist with correct columns; cell counts plausible (not zero, not thousands per site)"
    status: pending
    complexity: high
    priority: high
  - id: "loop-240-7"
    content: "Run feature extraction stage: cptools2 pipeline test_config.yaml --stages feature_extract. Verify index.csv generated by make_deepprofiler_index.py, weights bind-mounted via --bind, DeepProfiler profile completes, output at output_dir/{plate_id}/features/ with .npz files shape (n_cells, 672)"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "make_deepprofiler_index.py produces valid index.csv; DeepProfiler job runs on gpu-a100 PE; .npz features files exist with correct shape; no NaN/Inf in features"
    status: pending
    complexity: high
    priority: high
  - id: "loop-240-8"
    content: "Run full pipeline end-to-end: cptools2 pipeline test_config.yaml (no --stages). Wait for all SGE jobs including STAGE_IN + STAGE_OUT. Verify every stage produces expected outputs; nextflow report.html and timeline.html generated"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "All stages complete; all SGE jobs exit_status=0 in qacct; nextflow pipeline_info/report.html and timeline.html present; STAGE_OUT rsyncs to DataStore successfully"
    status: pending
    complexity: high
    priority: high
  - id: "loop-240-9"
    content: "Verify scratch quota compliance after full run: check lfs quota or df on /exports/eddie/scratch/$USER to confirm usage stayed within 75% target. Record any scratch hot spots"
    skill: "verification-before-completion"
    agent: "ralph-loop-worker"
    outcome: "Scratch usage reported at or below 75% of quota; result recorded in dev_docs/eddie_pipeline_gotchas.md"
    status: pending
    complexity: low
    priority: medium
  - id: "loop-240-10"
    content: "Write Loop 240 retrospective at dev_docs/retros/{date}_loop_240_eddie_validation.md. Include: job IDs + wall times per stage, any gotchas encountered, batch size tuning observations for cellpose, GPU queue wait times, output directory sizes, QC checks on feature shape and DMSO clustering if applicable"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "Retrospective file exists in dev_docs/retros/; contains job ID table, gotchas, observations; committed to git (if retros are tracked) or kept local (if gitignored)"
    status: pending
    complexity: low
    priority: medium
```
